from sqlalchemy import Integer, Column, String, ForeignKey, DateTime
from sqlalchemy.orm import relationship, backref
from database import Base
from table_post import Post
from table_user import User

class Feed(Base):
    __tablename__ = "feed_action"
    #__table_args__ = {"schema": "cd"}
    user_id  = Column(Integer, ForeignKey(User.id), primary_key=True)
    post_id  = Column(Integer, ForeignKey(Post.id), primary_key= True)
    action = Column(String)
    time = Column(DateTime)
    user = relationship(User, backref="Feed")
    post = relationship(Post, backref="Feed")