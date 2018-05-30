import pandas as pd
from sqlalchemy import create_engine
import re
import math
import numpy as np
import sqlite3
from global_config import *
from sqlalchemy import create_engine
from sqlalchemy import event, Column, Integer, String, ForeignKey
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship


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
    availiability = Column("availiability", String)


class Index(Base):
    __tablename__ = "index_info"

    course_index = Column("course_index", Integer, primary_key=True)
    course_code = Column("course_code", String, ForeignKey("course_info.course_code", onupdate='CASCADE', ondelete='CASCADE'))


class Class(Base):
    __tablename__ = "class_info"

    class_id = Column("class_id", Integer, primary_key=True)
    class_type = Column("class_type", String)
    class_group = Column("class_group", String)
    day = Column("day", String)
    start_time = Column("start_time", Integer)
    end_time = Column("end_time", Integer)
    venue = Column("venue", String)
    remark = Column("remark", String)
    course_index = Column("course_index", Integer, ForeignKey("index_info.course_index", onupdate='CASCADE', ondelete='CASCADE'))


class Program(Base):
    __tablename__ = "programs"

    program_description = Column("program_description", String, primary_key=True)
    program_code = Column("program_code", String)
    program_category = Column("program_category", String)


class Category(Base):
    __tablename__ = "course_category"

    id = Column("id", Integer, primary_key=True)
    course_code = Column("course_code", String)
    course_category = Column("course_category", String, ForeignKey("programs.program_description", onupdate='CASCADE', ondelete='CASCADE'))


engine = create_engine(course_sqlite_database_loc(), echo=False)
Base.metadata.create_all(bind=engine)
# Session = sessionmaker(bind=engine)
# session = Session()


def class_schedule_url(course_code):
    return "https://wish.wis.ntu.edu.sg/webexe/owa/AUS_SCHEDULE.main_display1?acadsem=" + acad_year + ";" + acad_sem + "&r_search_type=F&r_subj_code=" + course_code + "&boption=Search&staff_access=false"


def program_url(program_code):
    return "https://wish.wis.ntu.edu.sg/webexe/owa/AUS_SCHEDULE.main_display1?acadsem=" + acad_year + ";" + acad_sem + "&r_search_type=P&r_course_yr=" + program_code + "&boption=Cload&staff_access=false"


def errorMsg(loc):
    print("***" * 50)
    print("There is an error here on", loc)
    print("***" * 50)


def progressMsg(loc):
    print("Processing: ", loc)


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
                return np.nan
            return coursename[i + 1:].replace(")", "")


def courseInfoDF(df):
    course_info = pd.DataFrame({
        "course_code": [df.iat[0, 0]],
        "course_name": [coursenameConverter(df.iat[0, 1])],
        "acad_unit": [acadUnitConverter(df.iat[0, 2])],
        "availiability": [course_availiability(df.iat[0, 1])],
    })
    return course_info


def indexInfoDF(df1, df2):
    index_list = df2[0].tolist()
    index_info_dict = {
        "course_index": [int(index) for index in index_list[1:] if index == index],
        "course_code": []
    }
    index_info = pd.DataFrame.from_dict(index_info_dict, orient='index')
    index_info = index_info.replace(np.nan, df1.iat[0, 0])
    return index_info.transpose()


def classInfoDF(df1, df2):
    indexes = df2[0].tolist()
    index_list = []
    for index in indexes[1:]:
        if index == index:
            temp_index = index
        index_list.append(temp_index)

    start_time = [i[:4] if i == i else np.nan for i in df2[4][1:]]
    end_time = [i[-4:] if i == i else np.nan for i in df2[4][1:]]

    class_info = {
        "class_type": df2[1][1:],
        "class_group": df2[2][1:],
        "day": df2[3][1:],
        "start_time": start_time,
        "end_time": end_time,
        "venue": df2[5][1:],
        "remark": df2[6][1:],
        "course_index": index_list
    }
    return pd.DataFrame(class_info)


def programInfoDF(program_description, program_code, program_category):
    return pd.DataFrame({
        "program_description": [program_description],
        "program_code": [program_code],
        "program_category": [program_category]
    })


def categoryInfoDF(course_code, course_category):
    return pd.DataFrame({
        "course_code": [course_code],
        "course_category": [course_category]
    })


def storeCourseData(df1, df2):
    '''Store data in course_info, index_info and class_info tables.'''
    try:
        courseInfoDF(df1).to_sql("course_info", con=engine, if_exists='append', index=False)
        indexInfoDF(df1, df2).to_sql("index_info", con=engine, if_exists='append', index=False)
        classInfoDF(df1, df2).to_sql("class_info", con=engine, if_exists='append', index=False)
        return True
    except:
        errorMsg("database. Possibly conflict of unique values.")
        return False


def scrapeAndStoreCourseData(coursename):
    '''Scrape data and store data in course_info, index_info and class_info tables.'''
    engine = create_engine(course_sqlite_database_loc(), echo=False)
    try:
        df1, df2 = pd.read_html(class_schedule_url(coursename))
    except:
        errorMsg(coursename)
        return False
    else:
        storeCourseData(df1, df2)
        return True


def storeProgramData(program_description, program_code, program_category, coursecode):
    '''Store data in course_category and programs tables.'''
    engine = create_engine(course_sqlite_database_loc(), echo=False)
    try:
        programInfoDF(program_description, program_code, program_category).to_sql("programs", con=engine, if_exists='append', index=False)
    except:
        pass

    try:
        categoryInfoDF(coursecode, program_description).to_sql("course_category", con=engine, if_exists='append', index=False)
        return True
    except:
        errorMsg(program_description)
        return False


if __name__ == '__main__':
    # Store data to course_info, index_info and class_info tables for all course codes.
    for course in course_list:
        progressMsg(course)
        scrapeAndStoreCourseData(course)

    # Store data to course_category and programs tables for all programs under all categories.
    for i in range(len(program_names)):
        for j in range(len(des[i])):
            progressMsg(des[i][j])
            try:
                df_l = pd.read_html(program_url(code[i][j]))
                if len(df_l) % 2:
                    errorMsg(des[i][j])
                    continue
            except:
                errorMsg(des[i][j])
                continue
            coursecode_list = [df_l[i].iat[0, 0] for i in range(0, len(df_l), 2)]
            for coursecode in coursecode_list:
                progressMsg(coursecode)
                storeProgramData(des[i][j], code[i][j], program_names[i], coursecode)
