用命令将lib下的jar包放到本地仓库中
mvn install:install-file -Dfile=C:\Users\lx\Desktop\HttpClient-4.2.jar -DgroupId=com.golaxy -DartifactId=HttpClient -Dversion=4.2 -Dpackaging=jar -DgeneratePom=true