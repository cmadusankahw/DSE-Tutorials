# scala-spark-recommendation-system

Recommendation systems can be defined as software applications that draw out and learn from data such as preferences, their actions (clicks, for example), browsing history, and generated recommendations, which are products that the system determines are appealing to the user in the immediate future. This project contains a simple recommendation system build with Spark and Scala.

## Instructions to build

### Requirements
  Java Development Kit(JDK) > 1.8  
  Spark> 2.11+  
  Scala> 2.9+  
  sbt  
  Maven  
  InteliJ IDE (Preferred)  
  
### Step 1:
  Clone the Repository
 
### Step 2:
  Open project with InteliJ IDE (Or similar)

### Build project in IDE
  If dependencies are not auto downloaded, manually download them.
  
## Instruction to Run

  > This project is built with sbt  
  > Invoke the **sbt compile** project at the root folder project directory  
  > Besides loading build.sbt, the compile task is also loading settings from assembly.sbt  
  > Run **sbt assembly**

## Deployment

  > **spark-submit --class "package-name" --master local[2] --deploy-mode client --driver-memory 16g -num-executors 2 --executor-memory 2g --executor-cores 2  
  
## Issues & Contributing

  IF any clarifications contact me, or feel free to open an issue.
  
Thanks!
