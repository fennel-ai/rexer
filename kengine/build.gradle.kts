import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.5.31"
    kotlin("plugin.serialization") version "1.5.31"
    application
    java
}

group = "me.abhay"
version = "1.0-SNAPSHOT"

val ktorVersion = "1.6.6"
val logbackVersion = "1.2.7"
val kotlinxSerializationVersion = "1.3.1"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
    testImplementation(kotlin("test"))
    testImplementation("io.ktor:ktor-server-test-host:$ktorVersion")
    testImplementation("org.jetbrains.kotlin:kotlin-test")

    implementation(kotlin("stdlib"))
    implementation("io.ktor:ktor-server-core:$ktorVersion")
    implementation("io.ktor:ktor-server-netty:$ktorVersion")
    implementation("io.ktor:ktor-serialization:$ktorVersion")
    implementation("ch.qos.logback:logback-classic:$logbackVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:$kotlinxSerializationVersion")
}


tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "16"
}

application {
//    mainClass.set("ai.fennel.engine.StarqlServerKt")
    mainClass.set("MainKt")
}

java {
    sourceCompatibility = JavaVersion.VERSION_16
}

task("bench", JavaExec::class) {
    main = "MainKt"
    classpath = sourceSets["main"].runtimeClasspath
}
