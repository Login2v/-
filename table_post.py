from sqlalchemy import Integer, Column, String, desc
from sqlalchemy.orm import relationship
from database import Base, SessionLocal





class Post(Base):
    __tablename__ = "post"
    #__table_args__ = {"schema": "cd"}
    id = Column(Integer, primary_key=True)
    text = Column(String)
    topic = Column(String)
    #feed = relationship(Feed, back_populates='feed')

if __name__ == "__main__":
    session = SessionLocal()
    response = []
    for records in session.query(Post).filter(Post.topic == "business").order_by(desc(Post.id)).limit(10).all():
        response.append(records.id)
    print(response)