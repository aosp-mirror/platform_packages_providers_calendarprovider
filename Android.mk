LOCAL_PATH:= $(call my-dir)
include $(CLEAR_VARS)

LOCAL_MODULE_TAGS := user

LOCAL_SRC_FILES := $(call all-subdir-java-files)

# TODO: Remove dependency of application on the test runner (android.test.runner) 
# library.
LOCAL_JAVA_LIBRARIES := ext android.test.runner

# We depend on googlelogin-client also, but that is already being included by google-framework
LOCAL_STATIC_JAVA_LIBRARIES := google-framework

LOCAL_PACKAGE_NAME := CalendarProvider

include $(BUILD_PACKAGE)

# Use the following include to make our test apk.
include $(call all-makefiles-under,$(LOCAL_PATH))
