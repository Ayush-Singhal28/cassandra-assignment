package edu.knoldus

import com.datastax.driver.core.{ConsistencyLevel, Session}



object CassandraQuery extends App with CassandraProvider {

  val operation = new Operation
  cassandraSession.getCluster.getConfiguration.getQueryOptions.setConsistencyLevel(ConsistencyLevel.QUORUM)


  val empId = 3
  val empId1 = 1
  val empSalary1 = 50000
  val empSalary = 30000
  val empCity = "Chandigarh"

  operation.creatingEmployeeTable(cassandraSession)
  operation.insertingDataToTable(cassandraSession)
  operation.fetchingDatabyId(cassandraSession, empId)
  operation.updatingData(cassandraSession, empId1, empSalary1)
  operation.gettingDataBySalary(cassandraSession, empSalary, empId1)
  operation.gettingDataByCity(cassandraSession, empCity)
  operation.deletingDataByCity(cassandraSession, empCity)



}
