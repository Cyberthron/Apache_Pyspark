#Call the CSV file from local machine

titanic_df=spark.read.csv(r'F:\Apache_Pyspark\train.csv',inferSchema=True,header=True)

#Details about columns

titanic_df.printSchema()

#Columns present in CSV

titanic_df.columns

#Numbers of rows or records are present

titanic_df.count()

#Find the Number of columns present

len(titanic_df.columns)

# Describe summery of particular column

titanic_df.describe('Survived').show()

#Pclass Male Pessenngers who survived 

titanic_df.select("PassengerId","Survived","Pclass","Name","Age","Sex").filter("Sex='male' and Age>18 and Pclass=3 and Survived=1").show(50,False)

#Pclass 3 Female Pessengers who survived

titanic_df.select("PassengerId","Survived","Pclass","Name","Age","Sex").filter("Sex='female' and Age>18 and Pclass=3 and Survived=1").show()

#Pclass 3 Children who survived

titanic_df.select("PassengerId","Survived","Pclass","Name","Age","Sex").filter("Age>1 and Age<18 and Pclass=3 and Survived=1").show()
