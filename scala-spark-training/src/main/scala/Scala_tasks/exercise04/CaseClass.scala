package Scala_tasks.exercise04

import org.apache.log4j.{Level, Logger}

import scala.util.Random


object CaseClass {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // In a case class, we don't need to explicitly set getters setters
    // All Java bean class features will be included
    case class Employee(name: String, age: Int)

    def main(args: Array[String]) {
      val NUM_EMPLOYEES = 5
      val firstNames = List("Bruce", "Great", "The", "Jackie")
      val lastNames = List("Lee", "Khali", "Rock", "Chan")
      val employees = (0 until NUM_EMPLOYEES) map { i =>
        val first = Random.shuffle(firstNames).head
        val last = Random.shuffle(lastNames).head
        val fullName = s"$last, $first"
        val age = 20 + Random.nextInt(40)
        Employee(fullName, age)
      }

      // printing all employees
      employees foreach println

      val hasLee = """(Lee).*""".r
      for (employee <- employees) {
        employee match {
          case Employee(hasLee(x), age) => println("Found a Lee!")
          case _ => // Do nothing
        }
      }
    }
}
