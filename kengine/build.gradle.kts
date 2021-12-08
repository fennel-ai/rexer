import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.5.10"
    application
}

group = "me.abhay"
version = "1.0-SNAPSHOT"

val ktor_version = "1.6.6"
val logback_version = "1.2.5"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
    testImplementation(kotlin("test"))
    testImplementation "io.ktor:ktor-server-test-host:$ktor_version"
    testImplementation "org.jetbrains.kotlin:kotlin-test"

    implementation(kotlin("stdlib"))

    implementation("io.ktor:ktor-server-core:$ktor_version")
    implementation("io.ktor:ktor-server-netty:$ktor_version")
    implementation "io.ktor:ktor-serialization:$ktor_version"

    implementation("ch.qos.logback:logback-classic:$logback_version")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "16"
}

application {
    mainClass.set("ai.fennel.engine.StarqlServerKt")
}
