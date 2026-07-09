import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.mavenPublish)
    alias(libs.plugins.dokka)
    alias(libs.plugins.signing)
    alias(libs.plugins.nexusPublish)
}

version = System.getenv("VERSION") ?: "0.0.0-SNAPSHOT"

repositories {
    mavenCentral()
    mavenLocal()
    maven {
        url = uri("https://maven.pkg.github.com/OyabunAB/aelv")
        credentials {
            username = findProperty("gpr.user") as String? ?: System.getenv("GITHUB_ACTOR")
            password = findProperty("gpr.key") as String? ?: System.getenv("GITHUB_TOKEN")
        }
    }
}

dependencies {
    api(libs.kafka.clients)
    api(libs.aelv)
    implementation(libs.coroutines.core)
    implementation(libs.bundles.logging)
    testImplementation(libs.bundles.test)
}

val isRelease: Boolean = Regex("""^\d+\.\d+\.\d+$""").matches(version.toString())
val ossrhUsername: String? = System.getenv("OSSRH_USERNAME")
val ossrhPassword: String? = System.getenv("OSSRH_PASSWORD")
val signingKey: String? = System.getenv("GPG_SIGNING_KEY")
val signingPassword: String? = System.getenv("GPG_SIGNING_PASSWORD")

kotlin {
    jvmToolchain(libs.versions.jvm.get().toInt())
    compilerOptions { optIn.add("kotlin.time.ExperimentalTime") }
}

if (isRelease && signingKey != null) {
    signing {
        useInMemoryPgpKeys(signingKey, signingPassword)
        sign(publishing.publications["maven"])
    }
}

if (isRelease) {
    nexusPublishing {
        repositories {
            sonatype {
                username.set(ossrhUsername)
                password.set(ossrhPassword)
            }
        }
    }
}

val target = JvmTarget.fromTarget(libs.versions.jvm.get())
tasks.compileKotlin { compilerOptions { jvmTarget.set(target) } }
tasks.compileTestKotlin { compilerOptions { jvmTarget.set(target) } }

tasks.test {
    useJUnitPlatform()
    filter { includeTestsMatching("*Test*") }
    testLogging {
        events("started", "passed", "skipped", "failed")
        showStandardStreams = true
    }
}

val sourcesJar = tasks.register<Jar>("sourcesJar") {
    archiveClassifier = "sources"
    from(sourceSets.main.map { it.allSource })
}

val javadocJar = tasks.register<Jar>("javadocJar") {
    dependsOn(tasks.dokkaGenerate)
    archiveClassifier = "javadoc"
    from(layout.buildDirectory.dir("dokka/html"))
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
            artifact(sourcesJar)
            artifact(javadocJar)
            pom {
                name = "prozess"
                description = "Reactive Kafka streaming library for consumers and producers."
                url = "https://github.com/OyabunAB/prozess"
                licenses {
                    license {
                        name = "Apache License, Version 2.0"
                        url = "https://www.apache.org/licenses/LICENSE-2.0"
                    }
                }
                scm {
                    connection = "scm:git:git://github.com/OyabunAB/prozess.git"
                    developerConnection = "scm:git:ssh://github.com:OyabunAB/prozess.git"
                    url = "https://github.com/OyabunAB/prozess"
                }
                developers {
                    developer {
                        id = "dansun"
                        name = "Daniel Sundberg"
                        email = "daniel.sundberg@oyabun.se"
                    }
                }
            }
        }
    }
}
