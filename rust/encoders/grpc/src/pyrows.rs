// REQ-2194, REQ-2195: a row result read as it is. A driver that hands back Python rows (a DIRECT
// source, the response cache, an API source) has its rows encoded from those rows, never built
// into Arrow first: each cell is read here into the value the encoder writes, the value
// column_values would give of the cell had it been Arrow, and the encoder's writers take it from
// there as they take an Arrow cell. A value of a type read here by no rule (a range, a point, an
// address) is the text ir_arrow.text_of gives of it, as a row result's text has always been.

use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDate, PyDateAccess, PyDateTime, PyDelta, PyDeltaAccess, PyDict, PyFloat, PyInt,
    PyList, PyMemoryView, PyString, PyTime, PyTimeAccess, PyTuple, PyTzInfoAccess,
};

use arrow::datatypes::i256;

use crate::pyfmt::{Clock, DAY_US, MAX_DAYS, MAX_US, MIN_DAYS, MIN_US};
use crate::{Dec, Ts};

/// A cell of a row result: the Python value as the encoder reads one.
pub enum Pv {
    Null,
    Bool(bool),
    Int(i128),
    /// An int past i128, as its digits.
    BigInt(String),
    Float(f64),
    Dec(Dec),
    Str(String),
    Bin(Vec<u8>),
    /// days from 1970-01-01 and PostgreSQL's infinity (1, -1, else 0)
    Date(i64, i8),
    Ts(Ts),
    Time(Clock),
    /// ir_types.interval_parts: months, days, microseconds
    Interval(i64, i64, i64),
    List(Vec<Pv>),
    /// A dict with str keys, in its order.
    Dict(Vec<(String, Pv)>),
}

/// What reading a row result's cells needs from Python, looked up once per call.
pub struct Cells<'py> {
    decimal: Bound<'py, PyAny>,
    relativedelta: Bound<'py, PyAny>,
    interval_parts: Bound<'py, PyAny>,
    by_zoneinfo: Bound<'py, PyAny>,
    text_of: Bound<'py, PyAny>,
}

impl<'py> Cells<'py> {
    pub fn new(py: Python<'py>) -> PyResult<Self> {
        let ir_arrow = py.import("provisa.core.ir_arrow")?;
        Ok(Cells {
            decimal: py.import("decimal")?.getattr("Decimal")?,
            relativedelta: py.import("dateutil.relativedelta")?.getattr("relativedelta")?,
            interval_parts: py.import("provisa.core.ir_types")?.getattr("interval_parts")?,
            by_zoneinfo: ir_arrow.getattr("_by_zoneinfo")?,
            text_of: ir_arrow.getattr("text_of")?,
        })
    }

    /// The cell `v` of a column whose engine type is `kind` (None when the engine named none).
    pub fn read(&self, v: &Bound<'py, PyAny>, kind: Option<&str>) -> PyResult<Pv> {
        if v.is_none() {
            return Ok(Pv::Null);
        }
        if let Ok(s) = v.downcast::<PyString>() {
            return Ok(Pv::Str(s.to_str()?.to_owned()));
        }
        if v.is_instance_of::<PyBool>() {
            return Ok(Pv::Bool(v.extract()?));
        }
        if v.is_instance_of::<PyInt>() {
            return Ok(match v.extract::<i128>() {
                Ok(x) => Pv::Int(x),
                Err(_) => Pv::BigInt(v.str()?.to_str()?.to_owned()),
            });
        }
        if let Ok(f) = v.downcast::<PyFloat>() {
            return Ok(Pv::Float(f.value()));
        }
        if v.is_instance(&self.decimal)? {
            return self.decimal(v);
        }
        if let Ok(d) = v.downcast::<PyDateTime>() {
            return self.datetime(d);
        }
        if let Ok(d) = v.downcast::<PyDate>() {
            return Ok(match infinity(v)? {
                0 => Pv::Date(days(d.get_year(), d.get_month(), d.get_day()), 0),
                s if s > 0 => Pv::Date(MAX_DAYS, 1),
                _ => Pv::Date(MIN_DAYS, -1),
            });
        }
        if let Ok(t) = v.downcast::<PyTime>() {
            if t.get_tzinfo().is_none() {
                return Ok(Pv::Time(Clock { us: clock_us(t.get_hour(), t.get_minute(), t.get_second(), t.get_microsecond()), sub_ns: 0 }));
            }
        } else if let Ok(d) = v.downcast::<PyDelta>() {
            let total = if d.is_exact_instance_of::<PyDelta>() {
                (d.get_days() as i128 * 86_400 + d.get_seconds() as i128) * 1_000_000 + d.get_microseconds() as i128
            } else {
                // pandas' Timedelta: its nanoseconds dropped toward zero (ir_types.interval_parts)
                let ns: i128 = d.getattr("value")?.extract()?;
                ns.signum() * (ns.abs() / 1000)
            };
            let a = total.abs();
            let (days, us) = ((a / DAY_US as i128) as i64, (a % DAY_US as i128) as i64);
            return Ok(if total < 0 { Pv::Interval(0, -days, -us) } else { Pv::Interval(0, days, us) });
        } else if let Ok(b) = v.downcast::<PyBytes>() {
            return Ok(Pv::Bin(b.as_bytes().to_vec()));
        } else if let Ok(b) = v.downcast::<PyByteArray>() {
            return Ok(Pv::Bin(b.to_vec()));
        } else if v.is_instance(&self.relativedelta)? {
            let (m, d, us): (i64, i64, i64) = self.interval_parts.call1((v,))?.extract()?;
            return Ok(Pv::Interval(m, d, us));
        } else if v.is_instance_of::<PyList>() || v.is_instance_of::<PyTuple>() {
            let mut items = Vec::new();
            for item in v.try_iter()? {
                items.push(self.read(&item?, None)?);
            }
            return Ok(Pv::List(items));
        } else if let Ok(d) = v.downcast::<PyDict>() {
            if d.keys().iter().all(|k| k.is_instance_of::<PyString>()) {
                let mut entries = Vec::with_capacity(d.len());
                for (k, item) in d.iter() {
                    entries.push((k.downcast::<PyString>()?.to_str()?.to_owned(), self.read(&item, None)?));
                }
                return Ok(Pv::Dict(entries));
            }
        } else if v.is_instance_of::<PyMemoryView>() {
            return Ok(Pv::Bin(v.call_method0("tobytes")?.downcast::<PyBytes>()?.as_bytes().to_vec()));
        }
        Ok(Pv::Str(self.text_of.call1((v, kind))?.extract()?))
    }

    fn decimal(&self, v: &Bound<'py, PyAny>) -> PyResult<Pv> {
        let (sign, digits, exp): (u8, Bound<'py, PyTuple>, Bound<'py, PyAny>) = v.call_method0("as_tuple")?.extract()?;
        let Ok(exp) = exp.extract::<i32>() else {
            // NaN, sNaN or an infinity: numeric_out's text of it
            return Ok(Pv::Str(self.text_of.call1((v, None::<&str>))?.extract()?));
        };
        let mut text = String::with_capacity(digits.len() + 1);
        if sign == 1 {
            text.push('-');
        }
        for d in digits.iter() {
            text.push((b'0' + d.extract::<u8>()?) as char);
        }
        Ok(Pv::Dec(match text.parse::<i128>() {
            Ok(u) => Dec::D128(u, -exp),
            Err(_) => Dec::D256(
                i256::from_string(&text).ok_or_else(|| pyo3::exceptions::PyValueError::new_err(format!("Decimal {text}E{exp} is past 76 digits")))?,
                -exp,
            ),
        }))
    }

    fn datetime(&self, d: &Bound<'py, PyDateTime>) -> PyResult<Pv> {
        let v = d.as_any();
        let sign = infinity(v)?;
        if sign != 0 {
            let aware = d.get_tzinfo().is_some();
            return Ok(Pv::Ts(Ts {
                local: Clock { us: if sign > 0 { MAX_US } else { MIN_US }, sub_ns: 0 },
                offset: aware.then_some(0),
                inf: sign,
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
        let offset = match d.get_tzinfo() {
            None => None,
            Some(_) => {
                let off = d.call_method0("utcoffset")?;
                let off = off.downcast::<PyDelta>()?;
                Some(off.get_days() * 86_400 + off.get_seconds())
            }
        };
        // a pandas Timestamp keeps its nanoseconds past the microsecond
        let sub_ns = if offset.is_none() && !d.is_exact_instance_of::<PyDateTime>() {
            d.getattr("nanosecond").ok().map(|n| n.extract::<u32>()).transpose()?.unwrap_or(0)
        } else {
            0
        };
        Ok(Pv::Ts(Ts { local: Clock { us: local, sub_ns }, offset, inf: 0 }))
    }
}

/// ir_types.infinity_of: the IR infinite date's or timestamp's sign, else 0.
fn infinity(v: &Bound<'_, PyAny>) -> PyResult<i8> {
    if v.get_type().is(&v.py().get_type::<PyDateTime>()) || v.get_type().is(&v.py().get_type::<PyDate>()) {
        return Ok(0);
    }
    match v.getattr("pg_infinity") {
        Ok(s) => s.extract(),
        Err(_) => Ok(0),
    }
}

fn clock_us(h: u8, m: u8, s: u8, us: u32) -> i64 {
    ((h as i64 * 60 + m as i64) * 60 + s as i64) * 1_000_000 + us as i64
}

/// Days from 1970-01-01 of a proleptic Gregorian date.
fn days(y: i32, m: u8, d: u8) -> i64 {
    let (y, m, d) = (y as i64 - if m <= 2 { 1 } else { 0 }, m as i64, d as i64);
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::days;
    use crate::pyfmt::civil;

    #[test]
    fn days_is_civil_inverse() {
        for d in [-719_162i64, -1, 0, 1, 59, 60, 10_957, 2_932_896] {
            let (y, m, dd) = civil(d);
            assert_eq!(days(y as i32, m as u8, dd as u8), d);
        }
    }
}
