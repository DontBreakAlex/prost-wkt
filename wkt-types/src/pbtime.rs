use core::convert::TryFrom;
use core::str::FromStr;
use core::*;
use std::convert::TryInto;

use chrono::prelude::*;

#[cfg(feature = "diesel")]
use diesel::{data_types::PgTimestamp, deserialize::{self, FromSql}, pg::{Pg, PgValue}, serialize::{ToSql, Output, self}, sql_types::Timestamptz};
use serde::de::{self, Deserialize, Deserializer, Visitor};
use serde::ser::{Serialize, Serializer};

include!(concat!(env!("OUT_DIR"), "/pbtime/google.protobuf.rs"));
include!(concat!(env!("OUT_DIR"), "/prost_snippet.rs"));

/// Converts chrono's `NaiveDateTime` to `Timestamp`..
impl From<NaiveDateTime> for Timestamp {
    fn from(dt: NaiveDateTime) -> Self {
        Timestamp {
            seconds: dt.timestamp(),
            nanos: dt.timestamp_subsec_nanos() as i32,
        }
    }
}

/// Converts chrono's `DateTime<UTtc>` to `Timestamp`
impl From<DateTime<Utc>> for Timestamp {
    fn from(dt: DateTime<Utc>) -> Self {
        Timestamp {
            seconds: dt.timestamp(),
            nanos: dt.timestamp_subsec_nanos() as i32,
        }
    }
}

/// Converts proto timestamp to chrono's DateTime<Utc>
impl From<Timestamp> for DateTime<Utc> {
    fn from(val: Timestamp) -> Self {
        let mut value = val;
        // A call to `normalize` should capture all out-of-bound sitations hopefully
        // ensuring a panic never happens! Ideally this implementation should be
        // deprecated in favour of TryFrom but unfortunately having `TryFrom` along with
        // `From` causes a conflict.
        value.normalize();
        let dt = NaiveDateTime::from_timestamp_opt(value.seconds, value.nanos as u32)
            .expect("invalid or out-of-range datetime");
        DateTime::from_utc(dt, Utc)
    }
}

/// Converts proto duration to chrono's Duration
impl From<Duration> for chrono::Duration {
    fn from(val: Duration) -> Self {
        chrono::Duration::seconds(val.seconds) + chrono::Duration::nanoseconds(val.nanos as i64)
    }
}

/// Converts chrono Duration to proto duration
impl From<chrono::Duration> for Duration {
    fn from(val: chrono::Duration) -> Self {
        Duration {
            seconds: val.num_seconds(),
            nanos: (val.num_nanoseconds().unwrap() % 1_000_000_000) as i32,
        }
    }
}

impl Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        let mut ts = Timestamp {
            seconds: self.seconds,
            nanos: self.nanos,
        };
        ts.normalize();
        let dt: DateTime<Utc> = ts.try_into().map_err(serde::ser::Error::custom)?;
        serializer.serialize_str(format!("{dt:?}").as_str())
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        struct TimestampVisitor;

        impl<'de> Visitor<'de> for TimestampVisitor {
            type Value = Timestamp;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("Timestamp in RFC3339 format")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let utc: DateTime<Utc> = chrono::DateTime::from_str(value).map_err(|err| {
                    serde::de::Error::custom(format!(
                        "Failed to parse {value} as datetime: {err:?}"
                    ))
                })?;
                let ts = Timestamp::from(utc);
                Ok(ts)
            }
        }
        deserializer.deserialize_str(TimestampVisitor)
    }
}

#[cfg(feature = "diesel")]
impl FromSql<diesel::sql_types::Timestamp, Pg> for Timestamp {
    fn from_sql(bytes: PgValue<'_>) -> deserialize::Result<Self> {
        let PgTimestamp(mut offset) = FromSql::<diesel::sql_types::Timestamp, Pg>::from_sql(bytes)?;
        offset += 946728000000000;

        if offset >= -62135596800000000 && offset <= 253402300799999999 {
            Ok(Timestamp {
                seconds: offset / 1_000_000,
                nanos: ((offset % 1_000_000) * 1000) as i32,
            })
        } else {
            let message = "Tried to deserialize a timestamp that is too large for protobuf";
            Err(message.into())
        }
    }
}

#[cfg(feature = "diesel")]
impl ToSql<diesel::sql_types::Timestamp, Pg> for Timestamp {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        // The range of a protobuf timestamp is from 0001-01-01T00:00:00Z to 9999-12-31T23:59:59.999999999Z,
        // therefore, it will fit in a postgres timestamp.
        let mut microseconds = self.seconds * 1_000_000 + self.nanos as i64 / 1000;
        microseconds -= 946728000000000;

        ToSql::<diesel::sql_types::Timestamp, Pg>::to_sql(&PgTimestamp(microseconds), &mut out.reborrow())
    }
}

#[cfg(feature = "diesel")]
impl FromSql<Timestamptz, Pg> for Timestamp {
    fn from_sql(bytes: PgValue<'_>) -> deserialize::Result<Self> {
        FromSql::<diesel::sql_types::Timestamp, Pg>::from_sql(bytes)
    }
}

#[cfg(feature = "diesel")]
impl ToSql<Timestamptz, Pg> for Timestamp {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        ToSql::<diesel::sql_types::Timestamp, Pg>::to_sql(self, out)
    }
}

#[cfg(test)]
mod tests {

    use crate::pbtime::*;
    use chrono::{DateTime, Utc};

    #[test]
    fn serialize_duration() {
        let duration = Duration {
            seconds: 10,
            nanos: 100,
        };
        let json = serde_json::to_string_pretty(&duration).expect("json");
        println!("{json}");
        let back: Duration = serde_json::from_str(&json).expect("duration");
        assert_eq!(duration, back);
    }

    #[test]
    fn invalid_timestamp_test() {
        let ts = Timestamp {
            seconds: 10,
            nanos: 2000000000,
        };
        let datetime_utc: DateTime<Utc> = ts.into();

        println!("{datetime_utc:?}");
    }

    #[test]
    fn test_duration_conversion_pb_to_chrono() {
        let duration = Duration {
            seconds: 10,
            nanos: 100,
        };
        let chrono_duration: chrono::Duration = duration.into();
        assert_eq!(chrono_duration.num_seconds(), 10);
        assert_eq!((chrono_duration - chrono::Duration::seconds(10)).num_nanoseconds(), Some(100));
    }

    #[test]
    fn test_duration_conversion_chrono_to_pb() {
        let chrono_duration = chrono::Duration::seconds(10) + chrono::Duration::nanoseconds(100);
        let duration: Duration = chrono_duration.into();
        assert_eq!(duration.seconds, 10);
        assert_eq!(duration.nanos, 100);
    }
}
