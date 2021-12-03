create table forecast_app_project
(
    id                    serial       not null
        constraint forecast_app_project_pkey
            primary key,
    is_public             boolean      not null,
    name                  text         not null,
    time_interval_type    varchar(1)   not null,
    visualization_y_label text         not null,
    description           text         not null,
    home_url              varchar(200) not null,
    logo_url              varchar(200),
    core_data             varchar(200) not null,
    owner_id              integer
        constraint forecast_app_project_owner_id_c4918997_fk_auth_user_id
            references auth_user
            deferrable initially deferred
);

create table forecast_app_forecastmodel
(
    id           serial       not null
        constraint forecast_app_forecastmodel_pkey
            primary key,
    name         text         not null,
    abbreviation text         not null,
    team_name    text         not null,
    description  text         not null,
    home_url     varchar(200) not null,
    aux_data_url varchar(200),
    owner_id     integer
        constraint forecast_app_forecastmodel_owner_id_59518ca8_fk_auth_user_id
            references auth_user
            deferrable initially deferred,
    project_id   integer      not null
        constraint forecast_app_forecas_project_id_8a6a62fb_fk_forecast_
            references forecast_app_project
            deferrable initially deferred,
    citation     text,
    contributors text         not null,
    license      text         not null,
    methods      text,
    notes        text         not null,
    is_oracle    boolean      not null
);

create index forecast_app_forecastmodel_owner_id_59518ca8
    on forecast_app_forecastmodel (owner_id);

create index forecast_app_forecastmodel_project_id_8a6a62fb
    on forecast_app_forecastmodel (project_id);

create index forecast_app_project_owner_id_c4918997
    on forecast_app_project (owner_id);

create table forecast_app_target
(
    id                  serial  not null
        constraint forecast_app_target_pkey
            primary key,
    type                integer not null,
    name                text    not null,
    description         text    not null,
    is_step_ahead       boolean not null,
    numeric_horizon     integer,
    project_id          integer not null
        constraint forecast_app_target_project_id_eaadfc34_fk_forecast_
            references forecast_app_project
            deferrable initially deferred,
    outcome_variable    text    not null,
    reference_date_type integer
);

create index forecast_app_target_project_id_eaadfc34
    on forecast_app_target (project_id);

create table forecast_app_timezero
(
    id                serial  not null
        constraint forecast_app_timezero_pkey
            primary key,
    timezero_date     date    not null,
    data_version_date date,
    is_season_start   boolean not null,
    season_name       text,
    project_id        integer not null
        constraint forecast_app_timezer_project_id_a2dbaf7e_fk_forecast_
            references forecast_app_project
            deferrable initially deferred
);

create table forecast_app_forecast
(
    id                serial                   not null
        constraint forecast_app_forecast_pkey
            primary key,
    source            text                     not null,
    created_at        timestamp with time zone not null,
    forecast_model_id integer                  not null
        constraint forecast_app_forecas_forecast_model_id_b2b5025a_fk_forecast_
            references forecast_app_forecastmodel
            deferrable initially deferred,
    time_zero_id      integer                  not null
        constraint forecast_app_forecas_time_zero_id_21e20421_fk_forecast_
            references forecast_app_timezero
            deferrable initially deferred,
    notes             text,
    issued_at         timestamp with time zone not null,
    constraint unique_version
        unique (forecast_model_id, time_zero_id, issued_at)
);

create index forecast_app_forecast_forecast_model_id_b2b5025a
    on forecast_app_forecast (forecast_model_id);

create index forecast_app_forecast_time_zero_id_21e20421
    on forecast_app_forecast (time_zero_id);

create index forecast_app_forecast_issued_at_6844276b
    on forecast_app_forecast (issued_at);

create index forecast_app_timezero_project_id_a2dbaf7e
    on forecast_app_timezero (project_id);

create table forecast_app_unit
(
    id           serial  not null
        constraint forecast_app_unit_pkey
            primary key,
    name         text    not null,
    project_id   integer not null
        constraint forecast_app_unit_project_id_23eaa60c_fk_forecast_
            references forecast_app_project
            deferrable initially deferred,
    abbreviation text    not null,
    constraint unique_unit_abbreviation
        unique (project_id, abbreviation)
);

create table forecast_app_predictionelement
(
    id          serial      not null
        constraint forecast_app_predictionelement_pkey
            primary key,
    pred_class  integer     not null,
    is_retract  boolean     not null,
    data_hash   varchar(32) not null,
    forecast_id integer     not null
        constraint forecast_app_predict_forecast_id_e2ecf7cb_fk_forecast_
            references forecast_app_forecast
            deferrable initially deferred,
    target_id   integer     not null
        constraint forecast_app_predict_target_id_b372947f_fk_forecast_
            references forecast_app_target
            deferrable initially deferred,
    unit_id     integer     not null
        constraint forecast_app_predict_unit_id_d95aea34_fk_forecast_
            references forecast_app_unit
            deferrable initially deferred
);

create table forecast_app_predictiondata
(
    pred_ele_id integer not null
        constraint forecast_app_predictiondata_pkey
            primary key
        constraint forecast_app_predict_pred_ele_id_d9c30fd7_fk_forecast_
            references forecast_app_predictionelement
            deferrable initially deferred,
    data        jsonb   not null
);

create index forecast_app_predictionelement_target_id_b372947f
    on forecast_app_predictionelement (target_id);

create index forecast_app_predictionelement_unit_id_d95aea34
    on forecast_app_predictionelement (unit_id);

create index forecast_app_predictionelement_forecast_id_e2ecf7cb
    on forecast_app_predictionelement (forecast_id);

create index forecast_app_unit_project_id_23eaa60c
    on forecast_app_unit (project_id);
