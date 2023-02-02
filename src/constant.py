from enum import Enum

# sl core database
NOTIFICATION_COLL = 'notifications'
ANALYTIC_CONFIG_COLL = 'analytic-configs'
DATALOG_COLL = 'data-log'

# aidery auth database
AUTH_USER_COLL = 'users'
AUTH_DOMAIN_COLL = 'domains'
AUTH_PERSONAL_RECORDS_COLL = 'personal-records'

# aidery health database
HEALTH_RECORD_COLL = "health-records"
INFECT_LOCATIONS_COLL = 'infect-locations'
SURVEY_ANSWER_COLL = 'survey-answers'
SETTINGS_COLL = 'settings'
MASTER_SETTING_COLL = 'master-setting'
# WESAFE_USERS_COLL = 'users'

# aidery report database
DAILY_COLL = 'daily'
HEALTH_ANALYTIC_COLL = 'analytic-report'
NOTIFICATION_SUMMARISE_COLL = 'notification-summaries'
ACCESS_LOG_COLL = 'access-log'

# analytic process
HEALTH_RECORD_PRE_PROCESS_NAME = 'health-record-pre-process'
HEALTH_RECORD_DETECT_OUTLIER_PROCESS_NAME = 'health-record-detect-outlier-process'
HEALTH_INDICATOR_PROCESS_NAME = 'health-indicator-process'

# postgres database
POSTG_USER = 'users'


class EDailyReportCategory(Enum):
    Indicator = 1
    Health = 2
    Device = 3
    Activity = 4
    Movement = 5


# analytic type
class EAnalyticCategory(Enum):
    OVERVIEW = 0
    HEALTH_OUTLIER = 1

# Health records


class EHealthType(Enum):
    BODY_FAT = 0
    HEIGHT = 1
    BODY_MASS_INDEX = 2
    WAIST_CIRCUMFERENCE = 3
    STEPS = 4
    BASAL_ENERGY_BURNED = 5
    ACTIVE_ENERGY_BURNED = 6
    HEART_RATE = 7
    BODY_TEMPERATURE = 8
    BLOOD_PRESSURE_SYSTOLIC = 9   # / use for setting
    BLOOD_PRESSURE_DIASTOLIC = 10  # use for setting
    RESTING_HEART_RATE = 11
    WALKING_HEART_RATE = 12
    BLOOD_OXYGEN = 13
    BLOOD_GLUCOSE = 14
    ELECTRODERMAL_ACTIVITY = 15
    HIGH_HEART_RATE_EVENT = 16
    LOW_HEART_RATE_EVENT = 17
    IRREGULAR_HEART_RATE_EVENT = 18
    BLOOD_PRESSURE = 19  # value = SYSTOLIC = value2 = DIASTOLIC = value3 = HEART_RATE
    WEIGHT = 20
    DISTANCE = 21
    WATER = 22
    SLEEP = 23
    STAND_TIME = 24
    EXERCISE_TIME = 25
