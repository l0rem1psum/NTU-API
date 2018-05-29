import pandas as pd
from sqlalchemy import create_engine, event, Column, Integer, String, ForeignKey
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from global_config import *


def class_schedule_url(course_code):
    return "https://wish.wis.ntu.edu.sg/webexe/owa/AUS_SCHEDULE.main_display1?acadsem=" + acad_year + ";" + acad_sem + "&r_search_type=F&r_subj_code=" + course_code + "&boption=Search&staff_access=false"


def course_sqlite_database_loc():
    return "sqlite:///Database/Class_Schedule_" + acad_year + "_" + acad_sem + ".db"


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


Base = declarative_base()


class Course(Base):
    __tablename__ = "course_info"

    course_code = Column("course_code", String, primary_key=True)
    course_name = Column("course_name", String)
    acad_unit = Column("acad_unit", Integer)
    remark = Column("remark", String)


class Index(Base):
    __tablename__ = "index_info"

    course_index = Column("course_index", Integer, primary_key=True)
    course_code = Column("course_code", String, ForeignKey("course_info.course_code"))


class Class(Base):
    __tablename__ = "class_info"

    class_id = Column("class_id", Integer, primary_key=True)
    class_type = Column("class_type", String)
    class_group = Column("class_group", String)
    day = Column("day", String)
    start_time = Column("start_time", Integer)
    end_time = Column("end_time", Integer)
    venue = Column("venue", String)
    course_index = Column("course_index", Integer, ForeignKey("index_info.course_index"))


engine = create_engine(course_sqlite_database_loc(), echo=True)
Base.metadata.create_all(bind=engine)
Session = sessionmaker(bind=engine)

session = Session()

# courses = session.query(Course).all()
# for cor in courses:
#     print(cor.course_code)

course = Course()
# index = Index()
course.course_code = "CZ1003"
course.course_name = "Introduction To Computational Thinking"
course.acad_unit = 3
course.remark = "*"

# index.course_index = 12345
# index.course_code = "CZ1004"

session.add(course)
# session.add(index)
session.commit()

session.close()
