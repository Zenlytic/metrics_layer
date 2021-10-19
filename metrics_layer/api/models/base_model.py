from sqlalchemy.sql import func

from metrics_layer.api import db


class CoreMixin(object):
    last_modified = db.Column(
        db.DateTime, server_default=func.now(), onupdate=func.current_timestamp(), nullable=False
    )
    created_date = db.Column(db.DateTime, server_default=func.now(), nullable=False)

    @classmethod
    def create(cls, **kwargs):
        obj = cls(**kwargs)
        db.session.add(obj)
        db.session.commit()
        return obj

    @classmethod
    def modify(cls, id, update_dict):
        obj = cls.query.get(id)
        if obj is None:
            return
        else:
            for attr, value in update_dict.items():
                setattr(obj, attr, value)
            db.session.commit()
            return obj

    @classmethod
    def delete(cls, id):
        obj = cls.query.get(id)
        if obj is None:
            return
        else:
            db.session.delete(obj)
            db.session.commit()
            return obj
