plugins {
    java
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.postgresql:postgresql:42.7.7")
    implementation("com.zaxxer:HikariCP:6.3.0")
    implementation("org.springframework:spring-jdbc:6.2.8")
    implementation("org.springframework.boot:spring-boot-starter-jdbc:3.5.3")
    implementation("org.jooq:jooq:3.20.5")
}

tasks.register<JavaExec>("compatSmoke") {
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass = "com.hookwoods.pgkinetic.CompatibilitySmoke"
    jvmArgs("-Djava.util.logging.config.file=${project.file("logging.properties")}")
}
