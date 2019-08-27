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

User.create_table()
Post.create_table()
user1 = User(name='user1', email='elf_marsch_kompanija_kommandant_unter_schrift@mail.de')
user1.save()
# User.update(name = 'user2').filter(User.name == 'user1').call()
User.update(email = 'deutschen_soldaten_unter_ofizeren@mail.de').filter(User.email == 'elf_marsch_kompanija_kommandant_unter_schrift@mail.de')
Post.drop_table()
 
# print(User.get().call())

# User.delete().filter(User.name == 'user2').call()
