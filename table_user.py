from sqlalchemy import Integer, Column, String, func, and_, desc
from sqlalchemy.orm import relationship
from database import Base, SessionLocal

class User(Base):
    __tablename__ = "user"
    #__table_args__ = {"schema": "cd"}
    id = Column(Integer, primary_key=True)
    gender = Column(Integer)
    age = Column(Integer)
    country = Column(String)
    city = Column(String)
    exp_group = Column(Integer)
    os = Column(String)
    source = Column(String)

if __name__ == "__main__":
    session = SessionLocal()
    response = []
    for records in (session.query(User.country,User.os, func.count(User.id))
          .filter((and_(User.id) > 100), (User.exp_group) == 3)
          .group_by(User.country, User.os)
          .order_by(desc(func.count(User.id)))
          .all()
          ):
        if records[2]>100:
            response.append(records)
    print(response)