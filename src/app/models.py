import sqlalchemy as alq
from sqlalchemy import orm
import sqlalchemy.ext.declarative as alq_declarative

Base = alq_declarative.declarative_base()


class LoanSources(Base): 
    # Uno por cada tabla.  
    __tablename__ = 'loan_sources'

    id = alq.Column(alq.Integer, primary_key=True)
    name = alq.Column(alq.String(32))
  
    attributes = orm.relationship('LoanAttributes', 
            back_populates='loan_source', cascade="all, delete-orphan") 

    def __repr__(self): 
        return f"LoanSource(id={self.id}, name={self.name})"


class LoanAttributes(Base): 
    __tablename__ = 'loan_attributes'

    id      = alq.Column(alq.Integer, primary_key=True)
    name    = alq.Column(alq.String, nullable=False)
    dtype   = alq.Column(alq.String, nullable=False)
    alias   = alq.Column(alq.String, nullable=False)
    compare = alq.Column(alq.String)

    loan_source = orm.relationship('LoanSources', back_populates='attributes')

    def __repr__(self): 
        return f"LoanAttribute(id={self.id}, name={self.name}, alias={self.alias}, dtype={self.dtype})"



