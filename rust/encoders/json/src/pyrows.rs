//! REQ-2195: a row result read as it is. A driver that hands back Python rows (a DIRECT source,
//! the response cache, an API source) has its rows encoded from those rows, never built into Arrow
//! first: each cell is read here into the `P` the surfaces write, the value `column_values` gives
//! of a cell that holds it. A value of a type read here by no rule (a range, a point, an address)
//! is the text `ir_arrow.text_of` gives of it, as a row result's text has always been.

use std::borrow::Cow;

use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDate, PyDateAccess, PyDateTime, PyDelta, PyDeltaAccess, PyDict, PyFloat, PyInt,
    PyList, PyMemoryView, PyString, PyTime, PyTimeAccess, PyTuple, PyTzInfoAccess,
};

use crate::pyval::{Dec, Dt, Tz, Zone, P};

const DAY_US: i64 = 86_400_000_000;

/// What reading a row result's cells needs from Python, looked up once per call.
pub struct Cells<'py> {
    decimal: Bound<'py, PyAny>,
    uuid: Bound<'py, PyAny>,
    relativedelta: Bound<'py, PyAny>,
    interval_parts: Bound<'py, PyAny>,
    zoneinfo: Bound<'py, PyAny>,
    by_zoneinfo: Bound<'py, PyAny>,
    text_of: Bound<'py, PyAny>,
}

impl<'py> Cells<'py> {
    pub fn new(py: Python<'py>) -> PyResult<Self> {
        let ir_arrow = py.import("provisa.core.ir_arrow")?;
        Ok(Cells {
            decimal: py.import("decimal")?.getattr("Decimal")?,
            uuid: py.import("uuid")?.getattr("UUID")?,
            relativedelta: py.import("dateutil.relativedelta")?.getattr("relativedelta")?,
            interval_parts: py.import("provisa.core.ir_types")?.getattr("interval_parts")?,
            zoneinfo: py.import("zoneinfo")?.getattr("ZoneInfo")?,
            by_zoneinfo: ir_arrow.getattr("_by_zoneinfo")?,
            text_of: ir_arrow.getattr("text_of")?,
        })
    }

    /// A row result's batch, each row a sequence of `width` cells, as its columns' cells.
    pub fn columns(&self, rows: &Bound<'py, PyAny>, width: usize, kinds: &[Option<String>]) -> PyResult<Vec<Vec<P<'static>>>> {
        let mut columns: Vec<Vec<P<'static>>> = (0..width).map(|_| Vec::new()).collect();
        for row in rows.try_iter()? {
            let row = row?;
            if row.len()? < width {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "the row has {} columns, the plan {width}",
                    row.len()?
                )));
            }
            for (j, column) in columns.iter_mut().enumerate() {
                column.push(self.read(&row.get_item(j)?, kinds.get(j).and_then(|k| k.as_deref()))?);
            }
        }
        Ok(columns)
    }

    /// The cell `v` of a column whose engine type is `kind` (None when the engine named none).
    pub fn read(&self, v: &Bound<'py, PyAny>, kind: Option<&str>) -> PyResult<P<'static>> {
        if v.is_none() {
            return Ok(P::None);
        }
        if let Ok(s) = v.downcast::<PyString>() {
            return Ok(P::Str(Cow::Owned(match s.to_str() {
                Ok(t) => t.as_bytes().to_vec(),
                // a lone surrogate (a JSON escape's) kept as its three bytes, as WTF-8
                Err(_) => s.call_method1("encode", ("utf-8", "surrogatepass"))?.downcast::<PyBytes>()?.as_bytes().to_vec(),
            })));
        }
        if v.is_instance_of::<PyBool>() {
            return Ok(P::Bool(v.extract()?));
        }
        if v.is_instance_of::<PyInt>() {
            return Ok(match v.extract::<i128>() {
                Ok(x) => P::Int(x),
                Err(_) => P::BigInt(Cow::Owned(v.str()?.to_str()?.to_owned())),
            });
        }
        if let Ok(f) = v.downcast::<PyFloat>() {
            return Ok(P::Float(f.value()));
        }
        if v.is_instance(&self.decimal)? {
            return self.decimal(v);
        }
        if let Ok(d) = v.downcast::<PyDateTime>() {
            return self.datetime(d);
        }
        if let Ok(d) = v.downcast::<PyDate>() {
            return Ok(match infinity(v)? {
                0 => P::Date(days(d.get_year(), d.get_month(), d.get_day()) as i32),
                s => P::InfDate(s),
            });
        }
        if let Ok(t) = v.downcast::<PyTime>() {
            let us = clock_us(t.get_hour(), t.get_minute(), t.get_second(), t.get_microsecond());
            return Ok(match t.get_tzinfo() {
                None => P::Time(us),
                // a time with a zone, as a timestamp on 1970-01-01 at its offset (ir_arrow._clocks)
                Some(_) => {
                    let offset = utc_offset(v)?;
                    P::Dt(Dt { local_us: us, offset: Some(offset), tz: Tz::Py(offset) })
                }
            });
        }
        if let Ok(d) = v.downcast::<PyDelta>() {
            if d.is_exact_instance_of::<PyDelta>() {
                return Ok(P::Td((d.get_days() as i128 * 86_400 + d.get_seconds() as i128) * 1_000_000 + d.get_microseconds() as i128));
            }
            // pandas' Timedelta: floored to the microsecond
            let ns: i128 = d.getattr("value")?.extract()?;
            return Ok(P::Td(ns.div_euclid(1000)));
        }
        if let Ok(b) = v.downcast::<PyBytes>() {
            return Ok(P::Bytes(Cow::Owned(b.as_bytes().to_vec())));
        }
        if let Ok(b) = v.downcast::<PyByteArray>() {
            return Ok(P::Bytes(Cow::Owned(b.to_vec())));
        }
        if v.is_instance_of::<PyMemoryView>() {
            return Ok(P::Bytes(Cow::Owned(v.call_method0("tobytes")?.downcast::<PyBytes>()?.as_bytes().to_vec())));
        }
        if v.is_instance(&self.uuid)? {
            let b: Vec<u8> = v.getattr("bytes")?.extract()?;
            return Ok(P::Uuid(b.try_into().expect("a UUID's 16 bytes")));
        }
        if v.is_instance(&self.relativedelta)? {
            let (m, d, us): (i64, i64, i64) = self.interval_parts.call1((v,))?.extract()?;
            return Ok(P::Iv(m, d, us));
        }
        if v.is_instance_of::<PyTuple>() && v.get_type().name()? == "MonthDayNano" {
            // pyarrow's MonthDayNano (an interval inside a list or a struct, as to_pylist gives it)
            let (m, d, ns): (i32, i32, i64) = v.extract()?;
            return Ok(P::Mdn(m, d, ns));
        }
        if v.is_instance_of::<PyList>() || v.is_instance_of::<PyTuple>() {
            let mut items = Vec::new();
            for item in v.try_iter()? {
                items.push(self.read(&item?, None)?);
            }
            return Ok(P::List(items));
        }
        if let Ok(d) = v.downcast::<PyDict>() {
            if d.keys().iter().all(|k| k.is_instance_of::<PyString>()) {
                let mut entries = Vec::with_capacity(d.len());
                for (k, item) in d.iter() {
                    let P::Str(key) = self.read(&k, None)? else { unreachable!("a str key") };
                    entries.push((key, self.read(&item, None)?));
                }
                return Ok(P::Dict(entries));
            }
        }
        let text: String = self.text_of.call1((v, kind))?.extract()?;
        Ok(P::Str(Cow::Owned(text.into_bytes())))
    }

    fn decimal(&self, v: &Bound<'py, PyAny>) -> PyResult<P<'static>> {
        let (sign, digits, exp): (u8, Bound<'py, PyTuple>, Bound<'py, PyAny>) = v.call_method0("as_tuple")?.extract()?;
        let Ok(exp) = exp.extract::<i32>() else {
            // NaN, sNaN or an infinity: numeric_out's text of it
            let text: String = self.text_of.call1((v, None::<&str>))?.extract()?;
            return Ok(P::Str(Cow::Owned(text.into_bytes())));
        };
        let mut coef = String::with_capacity(digits.len());
        for d in digits.iter() {
            coef.push((b'0' + d.extract::<u8>()?) as char);
        }
        let coef = coef.trim_start_matches('0');
        let coef = if coef.is_empty() { "0".to_string() } else { coef.to_string() };
        // numeric has no negative zero
        let neg = sign == 1 && coef != "0";
        Ok(P::Dec(Dec { neg, coef: Cow::Owned(coef), exp }))
    }

    fn datetime(&self, d: &Bound<'py, PyDateTime>) -> PyResult<P<'static>> {
        let v = d.as_any();
        let sign = infinity(v)?;
        if sign != 0 {
            return Ok(P::InfDt(sign, d.get_tzinfo().is_some()));
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
        let local_us = days(d.get_year(), d.get_month(), d.get_day()) * DAY_US
            + clock_us(d.get_hour(), d.get_minute(), d.get_second(), d.get_microsecond());
        let (offset, tz) = match d.get_tzinfo() {
            None => (None, Tz::Naive),
            Some(tz) => {
                let offset = utc_offset(d.as_any())?;
                let named = if tz.is_instance(&self.zoneinfo)? {
                    let key: String = tz.getattr("key")?.extract()?;
                    match Zone::parse(&key) {
                        Ok(Zone::Named(_, name)) => Some(Tz::Named(name)),
                        _ => None,
                    }
                } else {
                    None
                };
                (Some(offset), named.unwrap_or(Tz::Py(offset)))
            }
        };
        Ok(P::Dt(Dt { local_us, offset, tz }))
    }
}

fn utc_offset(v: &Bound<'_, PyAny>) -> PyResult<i32> {
    let off = v.call_method0("utcoffset")?;
    let off = off.downcast::<PyDelta>()?;
    Ok(off.get_days() * 86_400 + off.get_seconds())
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
