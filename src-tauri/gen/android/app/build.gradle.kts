import java.io.File
import java.util.Properties
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
    id("rust")
}

val tauriProperties = Properties().apply {
    val propFile = file("tauri.properties")
    if (propFile.exists()) {
        propFile.inputStream().use { load(it) }
    }
}

val repositoryRoot = rootProject.projectDir.resolve("../../..").canonicalFile
val signingPropertiesFile = repositoryRoot.resolve("signing.properties")
val signingProperties = Properties()
if (!signingPropertiesFile.isFile) {
    throw GradleException("Android signing requires $signingPropertiesFile.")
}
signingPropertiesFile.inputStream().use { signingProperties.load(it) }

fun requireSigningValue(vararg names: String): String {
    return names.firstNotNullOfOrNull { name ->
        signingProperties.getProperty(name)?.trim()?.takeIf { it.isNotEmpty() }
    } ?: throw GradleException(
        "Missing Android signing property. Add one of: ${names.joinToString(", ")}",
    )
}

val signingKeystorePath =
    requireSigningValue("keystore.file", "keystore.path", "storeFile", "store.file")
val signingKeystoreFile = File(signingKeystorePath).let { candidate ->
    if (candidate.isAbsolute) candidate else repositoryRoot.resolve(signingKeystorePath)
}
if (!signingKeystoreFile.isFile) {
    throw GradleException("Android keystore does not exist: $signingKeystoreFile")
}
val signingStorePassword =
    requireSigningValue("keystore.password", "storePassword", "store.password")
val signingKeyAlias = requireSigningValue("key.alias", "keyAlias")
val signingKeyPassword = requireSigningValue("key.password", "keyPassword")

android {
    compileSdk = 36
    namespace = "com.ayangweb.eco_paste"
    defaultConfig {
        manifestPlaceholders["usesCleartextTraffic"] = "false"
        applicationId = "com.ayangweb.eco_paste"
        minSdk = 24
        targetSdk = 36
        versionCode = tauriProperties.getProperty("tauri.android.versionCode", "1").toInt()
        versionName = tauriProperties.getProperty("tauri.android.versionName", "1.0")
    }
    signingConfigs {
        create("shared") {
            storeFile = signingKeystoreFile
            storePassword = signingStorePassword
            keyAlias = signingKeyAlias
            keyPassword = signingKeyPassword
        }
    }
    buildTypes {
        getByName("debug") {
            manifestPlaceholders["usesCleartextTraffic"] = "true"
            isDebuggable = true
            isJniDebuggable = true
            isMinifyEnabled = false
            signingConfig = signingConfigs.getByName("shared")
            packaging {
                jniLibs.keepDebugSymbols.add("*/arm64-v8a/*.so")
                jniLibs.keepDebugSymbols.add("*/armeabi-v7a/*.so")
                jniLibs.keepDebugSymbols.add("*/x86/*.so")
                jniLibs.keepDebugSymbols.add("*/x86_64/*.so")
            }
        }
        getByName("release") {
            isMinifyEnabled = true
            signingConfig = signingConfigs.getByName("shared")
            proguardFiles(
                *fileTree(".") { include("**/*.pro") }
                    .plus(getDefaultProguardFile("proguard-android-optimize.txt"))
                    .toList().toTypedArray()
            )
        }
    }
    buildFeatures {
        buildConfig = true
    }
}

kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_1_8)
    }
}

rust {
    rootDirRel = "../../../"
}

dependencies {
    compileOnly(project(":hidden-api-stubs"))
    implementation("androidx.webkit:webkit:1.14.0")
    implementation("androidx.appcompat:appcompat:1.7.1")
    implementation("androidx.activity:activity-ktx:1.10.1")
    implementation("androidx.viewpager2:viewpager2:1.1.0")
    implementation("com.google.android.material:material:1.12.0")
    implementation("androidx.lifecycle:lifecycle-process:2.10.0")
    testImplementation("junit:junit:4.13.2")
    androidTestImplementation("androidx.test.ext:junit:1.1.4")
    androidTestImplementation("androidx.test.espresso:espresso-core:3.5.0")
}

apply(from = "tauri.build.gradle.kts")
