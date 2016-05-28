# Hive Masking

### Maven Script
> mvn archetype:generate -DgroupId=com.amazonaws.proserv -DartifactId=hive-masking -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false

> mvn package

### Function
> scp the jar to /usr/lib/hive/lib/

> use the Hive CLI to create the function or could be temporary function (dies after the session)

create temporary function md5mask as 'com.amazonaws.proserv.masking_hive.Md5Mask';
select page, md5mask(page) from pageviews;
