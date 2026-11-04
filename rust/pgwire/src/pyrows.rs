// REQ-2195: a row result read as it is. A driver that hands back Python rows (a DIRECT source,
// the response cache, an API source) has its rows encoded from those rows, never built into Arrow
// first: each cell is read here into the PyVal the column's writer takes, the same value the
// Arrow reader (pyval.rs) gives of a cell that holds it. A value of a type read here by no rule (a
// range, a point, an address) is the text ir_arrow.text_of gives of it, as a row result's text
// has always been.

use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDate, PyDateAccess, PyDateTime, PyDelta, PyDeltaAccess, PyDict, PyFloat, PyInt,
    PyList, PyMemoryView, PyString, PyTime, PyTimeAccess, PyTuple, PyTzInfoAccess,
};

use crate::pyval::{DateTime, Dec, DecSpecial, PyVal, RelDelta, TzRepr};
use crate::temporal::DAY_US;

/// What reading a row result's cells needs from Python, looked up once per call.
pub struct Cells<'py> {
    decimal: Bound<'py, PyAny>,
    uuid: Bound<'py, PyAny>,
    relativedelta: Bound<'py, PyAny>,
    exact_interval: Bound<'py, PyAny>,
    zoneinfo: Bound<'py, PyAny>,
    utc: Bound<'py, PyAny>,
    by_zoneinfo: Bound<'py, PyAny>,
    text_of: Bound<'py, PyAny>,
}

impl<'py> Cells<'py> {
    pub fn new(py: Python<'py>) -> PyResult<Self> {
        let ir_arrow = py.import("provisa.core.ir_arrow")?;
        let datetime = py.import("datetime")?;
        Ok(Cells {
            decimal: py.import("decimal")?.getattr("Decimal")?,
            uuid: py.import("uuid")?.getattr("UUID")?,
            relativedelta: py.import("dateutil.relativedelta")?.getattr("relativedelta")?,
            exact_interval: py.import("provisa.core.ir_types")?.getattr("_ExactInterval")?,
            zoneinfo: py.import("zoneinfo")?.getattr("ZoneInfo")?,
            utc: datetime.getattr("timezone")?.getattr("utc")?,
            by_zoneinfo: ir_arrow.getattr("_by_zoneinfo")?,
            text_of: ir_arrow.getattr("text_of")?,
        })
    }

    /// The cell `v` of a column whose engine type is `kind` (None when the engine named none).
    pub fn read(&self, v: &Bound<'py, PyAny>, kind: Option<&str>) -> PyResult<PyVal> {
        if v.is_none() {
            return Ok(PyVal::None);
        }
        if let Ok(s) = v.downcast::<PyString>() {
            return Ok(PyVal::Str(s.to_str()?.to_owned()));
        }
        if v.is_instance_of::<PyBool>() {
            return Ok(PyVal::Bool(v.extract()?));
        }
        if v.is_instance_of::<PyInt>() {
            return Ok(match v.extract::<i128>() {
                Ok(x) => PyVal::Int(x),
                Err(_) => PyVal::BigInt(v.str()?.to_str()?.to_owned()),
            });
        }
        if let Ok(f) = v.downcast::<PyFloat>() {
            return Ok(PyVal::Float(f.value()));
        }
        if v.is_instance(&self.decimal)? {
            return decimal(v);
        }
        if let Ok(d) = v.downcast::<PyDateTime>() {
            return self.datetime(d);
        }
        if let Ok(d) = v.downcast::<PyDate>() {
            let sign = infinity(v)?;
            let days = match sign {
                0 => days(d.get_year(), d.get_month(), d.get_day()),
                s if s > 0 => 2_932_896,
                _ => -719_162,
            };
            return Ok(PyVal::Date {
                days: days as i64,
                inf: sign,
            });
        }
        if let Ok(t) = v.downcast::<PyTime>() {
            let us = clock_us(t.get_hour(), t.get_minute(), t.get_second(), t.get_microsecond());
            return Ok(match t.get_tzinfo() {
                None => PyVal::Time(us as i64),
                // a time with a zone, as a timestamp on 1970-01-01 at its offset (ir_arrow._clocks)
                Some(_) => {
                    let off = v.call_method0("utcoffset")?;
                    let off = off.downcast::<PyDelta>()?;
                    let offset = off.get_days() * 86_400 + off.get_seconds();
                    PyVal::DateTime(DateTime {
                        local: us,
                        offset: Some(offset),
                        tz: fixed(offset),
                        inf: 0,
                        ns: None,
                    })
                }
            });
        }
        if let Ok(d) = v.downcast::<PyDelta>() {
            if d.is_exact_instance_of::<PyDelta>() {
                let us = (d.get_days() as i128 * 86_400 + d.get_seconds() as i128) * 1_000_000
                    + d.get_microseconds() as i128;
                return Ok(PyVal::TimeDelta(us));
            }
            // pandas' Timedelta
            return Ok(PyVal::PdTimedelta(Some(d.getattr("value")?.extract()?)));
        }
        if let Ok(b) = v.downcast::<PyBytes>() {
            return Ok(PyVal::Bytes(b.as_bytes().to_vec()));
        }
        if let Ok(b) = v.downcast::<PyByteArray>() {
            return Ok(PyVal::Bytes(b.to_vec()));
        }
        if v.is_instance_of::<PyMemoryView>() {
            return Ok(PyVal::Bytes(
                v.call_method0("tobytes")?.downcast::<PyBytes>()?.as_bytes().to_vec(),
            ));
        }
        if v.is_instance(&self.uuid)? {
            let b: Vec<u8> = v.getattr("bytes")?.extract()?;
            return Ok(PyVal::Uuid(b.try_into().expect("a UUID's 16 bytes")));
        }
        if v.is_instance(&self.relativedelta)? {
            let field = |name: &str| -> PyResult<i64> { v.getattr(name)?.extract() };
            let exact = if v.is_instance(&self.exact_interval)? {
                Some(v.getattr("pg_parts")?.extract()?)
            } else {
                None
            };
            return Ok(PyVal::Interval(RelDelta {
                years: field("years")?,
                months: field("months")?,
                days: field("days")?,
                hours: field("hours")?,
                minutes: field("minutes")?,
                seconds: field("seconds")?,
                microseconds: field("microseconds")?,
                exact,
            }));
        }
        if let Ok(l) = v.downcast::<PyList>() {
            let mut items = Vec::with_capacity(l.len());
            for item in l.iter() {
                items.push(self.read(&item, None)?);
            }
            return Ok(PyVal::List(items));
        }
        if let Ok(t) = v.downcast::<PyTuple>() {
            let mut items = Vec::with_capacity(t.len());
            for item in t.iter() {
                items.push(self.read(&item, None)?);
            }
            return Ok(PyVal::Tuple(items));
        }
        if let Ok(d) = v.downcast::<PyDict>() {
            let mut entries = Vec::with_capacity(d.len());
            for (k, item) in d.iter() {
                entries.push((self.read(&k, None)?, self.read(&item, None)?));
            }
            return Ok(PyVal::Dict(entries));
        }
        Ok(PyVal::Str(self.text_of.call1((v, kind))?.extract()?))
    }

    fn datetime(&self, d: &Bound<'py, PyDateTime>) -> PyResult<PyVal> {
        let v = d.as_any();
        let sign = infinity(v)?;
        if sign != 0 {
            // infinite_timestamp: datetime.max or min, in datetime.timezone.utc when aware
            let aware = d.get_tzinfo().is_some();
            let local = if sign > 0 {
                2_932_897 * DAY_US - 1
            } else {
                -719_162 * DAY_US
            };
            return Ok(PyVal::DateTime(DateTime {
                local,
                offset: aware.then_some(0),
                tz: if aware { TzRepr::PyUtc } else { TzRepr::Naive },
                inf: sign,
                ns: None,
            }));
        }
        // A datetime in a pytz zone is the same instant in that zone by zoneinfo (ir_arrow).
        let moved;
        let d = match d.get_tzinfo() {
            Some(tz) if tz.hasattr("zone")? => {
                moved = self.by_zoneinfo.call1((v,))?;
                moved.downcast::<PyDateTime>()?
            }
            _ => d,
        };
        let local = days(d.get_year(), d.get_month(), d.get_day()) * DAY_US
            + clock_us(d.get_hour(), d.get_minute(), d.get_second(), d.get_microsecond());
        let (offset, tz) = match d.get_tzinfo() {
            None => (None, TzRepr::Naive),
            Some(tz) => {
                let off = d.call_method0("utcoffset")?;
                let off = off.downcast::<PyDelta>()?;
                let offset = off.get_days() * 86_400 + off.get_seconds();
                let repr = if tz.is_instance(&self.zoneinfo)? {
                    TzRepr::ZoneInfo(tz.getattr("key")?.extract()?)
                } else if tz.is(&self.utc) {
                    TzRepr::PytzUtc
                } else {
                    fixed(offset)
                };
                (Some(offset), repr)
            }
        };
        // a pandas Timestamp keeps its nanoseconds past the microsecond
        let ns = if offset.is_none() && !d.is_exact_instance_of::<PyDateTime>() {
            d.getattr("nanosecond").ok().map(|n| n.extract::<u16>()).transpose()?
        } else {
            None
        };
        Ok(PyVal::DateTime(DateTime {
            local,
            offset,
            tz,
            inf: 0,
            ns,
        }))
    }
}

/// The repr pyarrow gives a fixed offset (pyval::tz_repr).
fn fixed(offset: i32) -> TzRepr {
    if offset == 0 {
        TzRepr::PytzUtc
    } else {
        TzRepr::PytzFixed(offset / 60)
    }
}

fn decimal(v: &Bound<'_, PyAny>) -> PyResult<PyVal> {
    let (sign, digits, exp): (u8, Bound<'_, PyTuple>, Bound<'_, PyAny>) = v.call_method0("as_tuple")?.extract()?;
    let neg = sign == 1;
    if let Ok(exp) = exp.extract::<i32>() {
        let mut text = Vec::with_capacity(digits.len());
        for d in digits.iter() {
            text.push(b'0' + d.extract::<u8>()?);
        }
        return Ok(PyVal::Dec(Dec {
            neg,
            digits: text,
            exp,
            special: DecSpecial::Finite,
        }));
    }
    let special = match exp.extract::<String>()?.as_str() {
        "F" => DecSpecial::Infinity,
        _ => DecSpecial::NaN, // "n" (NaN) and "N" (sNaN)
    };
    Ok(PyVal::Dec(Dec {
        neg,
        digits: vec![b'0'],
        exp: 0,
        special,
    }))
}

/// ir_types.infinity_of: the IR infinite date's or timestamp's sign, else 0.
fn infinity(v: &Bound<'_, PyAny>) -> PyResult<i32> {
    if v.get_type().is(&v.py().get_type::<PyDateTime>()) || v.get_type().is(&v.py().get_type::<PyDate>()) {
        return Ok(0);
    }
    match v.getattr("pg_infinity") {
        Ok(s) => s.extract(),
        Err(_) => Ok(0),
    }
}

fn clock_us(h: u8, m: u8, s: u8, us: u32) -> i128 {
    ((h as i128 * 60 + m as i128) * 60 + s as i128) * 1_000_000 + us as i128
}

/// Days from 1970-01-01 of a proleptic Gregorian date.
fn days(y: i32, m: u8, d: u8) -> i128 {
    let (y, m, d) = (y as i128 - if m <= 2 { 1 } else { 0 }, m as i128, d as i128);
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}
