LOCAL_PATH:= $(call my-dir)
include $(CLEAR_VARS)

# We only want this apk build for tests.
LOCAL_MODULE_TAGS := tests

# Include all test java files.
LOCAL_SRC_FILES := $(call all-java-files-under, src)

LOCAL_PACKAGE_NAME := CalendarProviderTests
LOCAL_PRIVATE_PLATFORM_APIS := true
LOCAL_COMPATIBILITY_SUITE := device-tests

LOCAL_STATIC_JAVA_LIBRARIES := calendar-common junit

LOCAL_JAVA_LIBRARIES := \
    ext \
    android.test.runner \
    android.test.base \
    android.test.mock \


LOCAL_INSTRUMENTATION_FOR := CalendarProvider

include $(BUILD_PACKAGE)
