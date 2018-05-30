import pandas as pd
from sqlalchemy import create_engine
import re
import math
import numpy as np
from global_config import *


def class_schedule_url(course_code):
    return "https://wish.wis.ntu.edu.sg/webexe/owa/AUS_SCHEDULE.main_display1?acadsem=" + acad_year + ";" + acad_sem + "&r_search_type=F&r_subj_code=" + course_code + "&boption=Search&staff_access=false"


def course_sqlite_database_loc():
    return "sqlite:///Database/Class_Schedule_" + acad_year + "_" + acad_sem + ".db"


def coursenameConverter(coursename):
    '''Convert all-capitalized course name to course name with only the first letter of each word capitalized.'''
    converted_coursename = re.sub("[^A-Za-z :,&?()'0-9]", "", coursename)
    return converted_coursename


def acadUnitConverter(acad_unit):
    return int(acad_unit[0])


def course_availiability(coursename):
    for i in range(-1, -6, -1):
        if coursename[i].isalnum():
            if i == -1:
                return ''
            return coursename[i + 1:]


def indexInfoDF(df1, df2):
    index_list = df2[0].tolist()
    index_info_dict = {
        "course_index": [int(index) for index in index_list if (index == index) and (index != 'INDEX')],
        "course_code": []
    }
    index_info = pd.DataFrame.from_dict(index_info_dict, orient='index')
    index_info = index_info.replace(np.nan, df1.iat[0, 0])
    return index_info.transpose()


def courseInfoDF(df):
    course_info = pd.DataFrame({
        "course_code": [df.iat[0, 0]],
        "course_name": [coursenameConverter(df.iat[0, 1])],
        "acad_unit": [acadUnitConverter(df.iat[0, 2])],
        "availiability": [course_availiability(df.iat[0, 1])],
    })
    return course_info


# def classInfoDF(df)


# df1, df2 = pd.read_html("https://wish.wis.ntu.edu.sg/webexe/owa/AUS_SCHEDULE.main_display1?acadsem=2018;1&r_search_type=F&r_subj_code=cz2005&boption=Search&staff_access=false")

# print(df1)
# print(courseInfoDF(df1))
# print(indexInfoDF(df1, df2))


# courseInfoDF(df1).to_sql("course_info", con=engine, if_exists='append', index=False)
# indexInfoDF(df1, df2).to_sql("index_info", con=engine, if_exists='append', index=False)
engine = create_engine(course_sqlite_database_loc(), echo=False)
l = ['AB0601', 'AB0901', 'AB1000', 'AB1201', 'AB1202', 'AB1301', 'AB1401', 'AB1402', 'AB1501', 'AB1601']
for mods in l:
    print("Storing data of course", mods)
    df1, df2 = pd.read_html("https://wish.wis.ntu.edu.sg/webexe/owa/AUS_SCHEDULE.main_display1?acadsem=2018;1&r_search_type=F&r_subj_code=" + mods + "&boption=Search&staff_access=false")
    courseInfoDF(df1).to_sql("course_info", con=engine, if_exists='append', index=False)
    indexInfoDF(df1, df2).to_sql("index_info", con=engine, if_exists='append', index=False)


####################################################################################


# from sqlalchemy import event, Column, Integer, String, ForeignKey
# from sqlalchemy.engine import Engine
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import sessionmaker, relationship


# @event.listens_for(Engine, "connect")
# def set_sqlite_pragma(dbapi_connection, connection_record):
#     cursor = dbapi_connection.cursor()
#     cursor.execute("PRAGMA foreign_keys=ON")
#     cursor.close()


# Base = declarative_base()


# class Course(Base):
#     __tablename__ = "course_info"

#     course_code = Column("course_code", String, primary_key=True)
#     course_name = Column("course_name", String)
#     acad_unit = Column("acad_unit", Integer)
#     availiability = Column("availiability", String)


# class Index(Base):
#     __tablename__ = "index_info"

#     course_index = Column("course_index", Integer, primary_key=True)
#     course_code = Column("course_code", String, ForeignKey("course_info.course_code", onupdate='CASCADE', ondelete='CASCADE'))


# class Class(Base):
#     __tablename__ = "class_info"

#     class_id = Column("class_id", Integer, primary_key=True)
#     class_type = Column("class_type", String)
#     class_group = Column("class_group", String)
#     day = Column("day", String)
#     start_time = Column("start_time", Integer)
#     end_time = Column("end_time", Integer)
#     venue = Column("venue", String)
#     course_index = Column("course_index", Integer, ForeignKey("index_info.course_index", onupdate='CASCADE', ondelete='CASCADE'))


# engine = create_engine(course_sqlite_database_loc(), echo=False)
# Base.metadata.create_all(bind=engine)
# Session = sessionmaker(bind=engine)

# session = Session()

# course = Course()
# course.course_code = "CZ1003"
# course.course_name = "Introduction To Computational Thinking"
# course.acad_unit = 3
# course.remark = "*"


# index = Index()
# index.course_index = 12345
# index.course_code = "CZ1004"

# session.add(course)
# session.add(index)

# session.commit()
# session.close()
