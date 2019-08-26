#!/usr/bin/env python3

from base import create_sqlite3_session, Base, Field, Integer, String

session = create_sqlite3_session('db')
Base.set_session(session)

# print(Integer)
# print(String(200))

class User(Base):

    __tablename__ = 'users'

    id = Field(
        type=Integer,
        primary=True,
        autoincrement=True,
    )
    name = Field(
        type=String(100),
        not_null=True,
    )
    email = Field(
        type=String(100),
        not_null=True,
    )

class Post(Base):

    __tablename__ = 'posts'

    id = Field(
        type=Integer,
        primary=True,
        autoincrement=True,
    )
    user_id = Field(
        type=Integer,
        foreign=User.id,
    )
    post = Field(
        type=String(100),
        not_null=False,
    )

#print([field[1].__dict__ for field in User.get_fields()])
#print([field[1].__dict__ for field in Post.get_fields()])
User.create_table()
Post.create_table()
user1 = User(name='user1', email='elf_marsch_kompania_kommandant_unter_schrift@mail.de')
user1.save()
# User.delete().filter(User.name == 'user2').call()
# User.drop_table()
