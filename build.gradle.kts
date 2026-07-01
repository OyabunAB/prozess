import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.mavenPublish)
}

version = libs.versions.prozess.get()

repositories {
    mavenCentral()
    mavenLocal()
}

configurations.all {
    resolutionStrategy.eachDependency {
        if (requested.group == "org.apache.kafka" && requested.name == "kafka-clients") {
            useVersion("3.9.2")
            because("CVE-2026-35554")
        }
    }
}

dependencies {
    api(libs.kafka.clients)
    api(libs.reactor.core)
    implementation(libs.kotlinx.coroutines.reactor)
    implementation(libs.bundles.logging)
    testImplementation(libs.bundles.test)
}

kotlin {
    jvmToolchain(libs.versions.jvm.get().toInt())
    compilerOptions { optIn.add("kotlin.time.ExperimentalTime") }
}

tasks {
    val target = JvmTarget.fromTarget(libs.versions.jvm.get())
    compileKotlin { compilerOptions { jvmTarget.set(target) } }
    compileTestKotlin { compilerOptions { jvmTarget.set(target) } }
    test {
        useJUnitPlatform()
        filter { includeTestsMatching("*Test*") }
        testLogging { events("passed", "skipped", "failed") }
    }
}

publishing {
    repositories {
        maven {
            name = "GitHubPackages"
            url  = uri("https://maven.pkg.github.com/oyabun/prozess")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
        }
    }
}
