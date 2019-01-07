from collections import defaultdict
from typing import List, Tuple, Dict

from datetime import datetime

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import false
from sqlalchemy.sql.functions import concat

from app.libs.zmon import MIN_VAL

from app.extensions import db


class Indicator(db.Model):
    id = db.Column(db.Integer(), primary_key=True)

    name = db.Column(db.String(120), nullable=False, index=True)
    source = db.Column(db.JSON(), nullable=False)
    unit = db.Column(db.String(20), nullable=False, default='')
    aggregation = db.Column(db.String(80), default='average')
    is_deleted = db.Column(db.Boolean(), default=False, index=True, server_default=false())

    product_id = db.Column(db.Integer(), db.ForeignKey('product.id'), nullable=False, index=True)

    slug = db.Column(db.String(120), nullable=False, index=True)

    targets = db.relationship('Target', backref=db.backref('indicator', lazy='joined'), lazy='dynamic')
    values = db.relationship('IndicatorValue', backref='indicator', lazy='dynamic', passive_deletes=True)
    compact_values = db.relationship('IndicatorValueCompact', backref='indicator', lazy='dynamic', passive_deletes=True)

    username = db.Column(db.String(120), default='')
    created = db.Column(db.DateTime(), default=datetime.utcnow)
    updated = db.Column(db.DateTime(), onupdate=datetime.utcnow, default=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint('name', 'product_id', 'is_deleted', name='indicator_name_product_id_key'),
    )

    def get_owner(self):
        return self.product.product_group.name

    def __repr__(self):
        return '<SLI {} | {} | {}>'.format(self.product.name, self.name, self.source)


class IndicatorValue(db.Model):
    __tablename__ = 'indicatorvalue'

    timestamp = db.Column(db.DateTime(), nullable=False)
    value = db.Column(db.Numeric(), nullable=False)

    indicator_id = db.Column(
        db.Integer(),
        db.ForeignKey('indicator.id', ondelete='CASCADE'),
        nullable=False, index=True)

    __table_args__ = (
        db.PrimaryKeyConstraint('timestamp', 'indicator_id', name='indicatorvalue_timestamp_indicator_id_pkey'),
    )

    def as_dict(self):
        return {
            'timestamp': self.timestamp,
            'value': self.value,
            'indicator_id': self.indicator_id
        }

    def update_dict(self):
        return {'value': self.value}

    def __repr__(self):
        return '<SLI value {} | {}: {}>'.format(self.indicator.name, self.timestamp, self.value)


# Source: http://stackoverflow.com/questions/41636169/how-to-use-postgresqls-insert-on-conflict-upsert-feature-with-flask-sqlal  # noqa
def insert_indicator_value(session: db.Session, sli_value: IndicatorValue) -> None:
    """
    Upsert indicator value.

    Note: Does not perform ``session.commit()``.
    """
    statement = (
        pg_insert(IndicatorValue)
        .values(**sli_value.as_dict())
        .on_conflict_do_update(constraint='indicatorvalue_timestamp_indicator_id_pkey', set_=sli_value.update_dict())
    )

    session.execute(statement)


########################################################################################################################
# COMPACTED SLI VALUES
########################################################################################################################

# Offset(minutes) from day start + Float
ValueUnit = Tuple[int, float]


class IndicatorValueCompact(db.Model):
    __tablename__ = 'indicatorvaluecompact'

    timebucket = db.Column(db.DateTime(), nullable=False, index=True)  # daily datetime
    values = db.Column(db.Text(), nullable=False)  # '<offset:value>,...' -> '0:1.0,953:3.5,'

    indicator_id = db.Column(
        db.Integer(),
        db.ForeignKey('indicator.id', ondelete='CASCADE'),
        nullable=False, index=True)

    __table_args__ = (
        db.PrimaryKeyConstraint(
            'timebucket', 'indicator_id', name='indicatorvaluecompact_timebucket_indicator_id_pkey'),
    )

    def get_indicator_values(self) -> List[IndicatorValue]:
        ivs: Dict[datetime, float] = defaultdict(float)
        [ivs.update({datetime.fromtimestamp(self.timebucket.timestamp() + int(v[0]) * 60): float(v[1])}) for v in
            [t.split(':') for t in self.values.split(',') if t]]

        return [IndicatorValue(timestamp=timestamp, value=value, indicator_id=self.indicator_id)
                for timestamp, value in ivs.items()]

    def __repr__(self):
        return '<SLI values {} | {} | {}>'.format(self.indicator.name, self.timebucket, len(self.values))


def update_indicator_value_compact(session: db.Session, indicator_id: int, results: Dict[datetime, float]):
    # session.query(User).update({ User.department: concat(PREFIX, User.department, SUFFIX)}, False)

    day_buckets = results_to_buckets(results)

    for timebucket, values in day_buckets.items():
        value_units_compact = ','.join(['{}:{}'.format(str(v[0]), str(v[1])) for v in values]) + ','

        statement = (
            pg_insert(IndicatorValueCompact)
            .values(**{
                'timebucket': timebucket, 'values': value_units_compact, 'indicator_id': indicator_id,
            })
            .on_conflict_do_update(
                constraint='indicatorvaluecompact_timebucket_indicator_id_pkey',
                set_={'values': concat(IndicatorValueCompact.values, value_units_compact)}
            )
        )

        session.execute(statement)


def results_to_buckets(results: Dict[datetime, float]) -> Dict[datetime, List[ValueUnit]]:
    buckets: Dict[datetime, List[ValueUnit]] = defaultdict(list)

    for minute, val in results.items():
        if val > 0:
            val = max(val, MIN_VAL)
        elif val < 0:
            val = min(val, MIN_VAL * -1)

        day = datetime(minute.year, minute.month, minute.day)
        offset = int((minute - day).total_seconds() / 60)
        buckets[day].append((offset, round(val, 3)))

    return buckets
