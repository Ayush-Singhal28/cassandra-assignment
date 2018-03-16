package edu.knoldus

import com.datastax.driver.core.Session
import scala.collection.JavaConverters._

class Operation {

  def creatingEmployeeTable(cassandraSession: Session): Unit = {
    cassandraSession.execute("create table if not exists employee(emp_id int ,emp_name text,emp_city text,emp_salary, varint,emp_phone varint, primary key(emp_id,emp_salary))")
  }

  def insertingDataToTable(cassandraSession: Session): Unit = {

    cassandraSession.execute("insert into employee(emp_id, emp_name, emp_city, emp_salary, emp_phone) VALUES(1, 'Ayush', 'Delhi', 50000, 9876543210)")
    cassandraSession.execute("insert into employee(emp_id, emp_name, emp_city, emp_salary, emp_phone) values( 2,'Vaibhav','Chandigarh',30000,9897888654)")
    cassandraSession.execute("insert into employee(emp_id, emp_name, emp_city, emp_salary, emp_phone) values(3,'Sagar','Banaras',70000,8450003459")
    cassandraSession.execute("insert into employee(emp_id, emp_name, emp_city, emp_salary, emp_phone) values(4,'Vatan','Chandigarh',20000,8278979990")
  }


  def fetchingDatabyId(cassandraSession: Session, emp_id: Int): Unit = {

    val result = cassandraSession.execute(s"select * from employee where emp_id=${emp_id}").asScala.toList
    result.foreach(println(_))

  }

  def updatingData(cassandraSession: Session, id: Int, salary: Int): Unit = {
    cassandraSession.execute(s"update employee set city='chandigarh' where emp_id=${id} AND salary=${salary}")

  }

  def gettingDataBySalary(cassandraSession: Session, salary: Int, id: Int): Unit = {
    val result = cassandraSession.execute(s"select * from employee where emp_id =${id} AND emp_salary > ${salary}")
    result.forEach(println(_))

  }

  def gettingDataByCity(cassandraSession: Session, city: String): Unit = {


    val result = cassandraSession.execute(s"select * from employee where emp_city ='${city}'").asScala.toList
    result.foreach(println(_))

  }

  private def deletingDataByCity(cassandraSession: Session, city: String): Unit = {
    cassandraSession.execute("create table newEmployee(emp_id int,emp_name text,emp_city text,emp_salary varint,emp_phone varint,primary key(emp_city));")
    cassandraSession.execute("insert into newEmployee(emp_id, emp_name, emp_city, emp_salary, emp_phone) VALUES(1, 'Ayush', 'Delhi', 50000, 9876543210)")
    cassandraSession.execute("insert into newEmployee(emp_id, emp_name, emp_city, emp_salary, emp_phone) values( 2,'Vaibhav','Chandigarh',30000,9897888654)")
    cassandraSession.execute("insert into newEmployee(emp_id, emp_name, emp_city, emp_salary, emp_phone) values(3,'Sagar','Banaras',70000,8450003459")
    cassandraSession.execute("insert into newEmployee(emp_id, emp_name, emp_city, emp_salary, emp_phone) values(4,'Vatan','Chandigarh',20000,8278979990")
    cassandraSession.execute(s"delete from newEmployee where emp_city = '${city}'")
    val result = cassandraSession.execute("select * from newEmployee").asScala.toList
    result.foreach(println(_))
  }



}
