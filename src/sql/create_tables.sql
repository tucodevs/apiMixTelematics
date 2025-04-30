CREATE TABLE tipos_eventos (
    EventTypeId BIGINT PRIMARY KEY,
    EventType VARCHAR(100),
    Description TEXT,
    DisplayUnits VARCHAR(50),
    FormatType VARCHAR(50),
    ValueName VARCHAR(100)
);

CREATE TABLE tr_excesso_velocidade_55km_2 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    AssetId BIGINT,
    DriverId BIGINT,
    EventId BIGINT,
    EventTypeId BIGINT,
    EventCategory VARCHAR(100),
    StartDateTime DATETIME,
    StartLatitude DECIMAL(10,8),
    StartLongitude DECIMAL(11,8),
    StartSpeedKph DECIMAL(6,2),
    StartOdometer DECIMAL(10,2),
    EndDateTime DATETIME,
    EndLatitude DECIMAL(10,8),
    EndLongitude DECIMAL(11,8),
    EndSpeedKph DECIMAL(6,2),
    EndOdometer DECIMAL(10,2),
    Value DECIMAL(10,6),
    FuelUsedLitres DECIMAL(10,6),
    ValueType VARCHAR(100),
    ValueUnits VARCHAR(100),
    TotalTimeSeconds INT,
    TotalOccurances INT,
    SpeedLimit INT,
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (EventTypeId) REFERENCES tipos_eventos(EventTypeId)
);

CREATE TABLE tr_fora_faixa_verde LIKE tr_excesso_velocidade_55km_2;
CREATE TABLE tr_excesso_velocidade_30km LIKE tr_excesso_velocidade_55km_2;
CREATE TABLE tr_batendo_transmissao LIKE tr_excesso_velocidade_55km_2;
CREATE TABLE tr_excesso_velocidade_40km_2 LIKE tr_excesso_velocidade_55km_2;
CREATE TABLE tr_embreagem_acionada_indevida LIKE tr_excesso_velocidade_55km_2;
CREATE TABLE tr_freada_brusca_grave LIKE tr_excesso_velocidade_55km_2;
CREATE TABLE tr_aceleracao_brusca LIKE tr_excesso_velocidade_55km_2;
CREATE TABLE tr_marcha_lenta_5min LIKE tr_excesso_velocidade_55km_2;
CREATE TABLE tr_excesso_velocidade_20km LIKE tr_excesso_velocidade_55km_2;
CREATE TABLE tr_freada_brusca LIKE tr_excesso_velocidade_55km_2;
CREATE TABLE tr_excesso_velocidade_60km LIKE tr_excesso_velocidade_55km_2;
CREATE TABLE tr_excesso_rpm_parado LIKE tr_excesso_velocidade_55km_2;
CREATE TABLE tr_marcha_lenta LIKE tr_excesso_velocidade_55km_2;
CREATE TABLE tr_curva_brusca LIKE tr_excesso_velocidade_55km_2;
CREATE TABLE tr_excesso_velocidade_50km LIKE tr_excesso_velocidade_55km_2;
CREATE TABLE tr_inercia_aproveitada LIKE tr_excesso_velocidade_55km_2;
CREATE TABLE tr_excesso_velocidade_40km_1 LIKE tr_excesso_velocidade_55km_2;
CREATE TABLE tr_excesso_velocidade_55km_1 LIKE tr_excesso_velocidade_55km_2;
CREATE TABLE tr_excesso_rotacao LIKE tr_excesso_velocidade_55km_2;

CREATE TABLE drivers (
    DriverId BIGINT PRIMARY KEY,
    OrganisationId BIGINT,
    Name VARCHAR(255),
    SiteId BIGINT,
    ImageUri VARCHAR(500),
    FmDriverId BIGINT,
    EmployeeNumber VARCHAR(100),
    IsSystemDriver BOOLEAN,
    MobileNumber VARCHAR(100),
    Email VARCHAR(255),
    ExtendedDriverId BIGINT,
    ExtendedDriverIdType VARCHAR(100),
    Country VARCHAR(100),
    AdditionalDetailFields JSON,
    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS assets (
    AssetId BIGINT PRIMARY KEY,
    AssetTypeId BIGINT,
    Description VARCHAR(255),
    IsConnectedTrailer BOOLEAN,
    RegistrationNumber VARCHAR(100),
    SiteId BIGINT,
    FuelType VARCHAR(100),
    FuelTankCapacity FLOAT,
    TargetFuelConsumption FLOAT,
    TargetFuelConsumptionUnits VARCHAR(50),
    TargetHourlyFuelConsumption FLOAT,
    TargetHourlyFuelConsumptionUnits VARCHAR(50),
    FleetNumber VARCHAR(100),
    WltpMaxRangeKm BIGINT,
    BatteryCapacitykWh BIGINT,
    UsableBatteryCapacitykWh BIGINT,
    Make VARCHAR(100),
    Model VARCHAR(100),
    Year VARCHAR(10),
    VinNumber VARCHAR(100),
    SerialNumber VARCHAR(100),
    AempEquipmentId VARCHAR(100),
    EngineNumber VARCHAR(100),
    DefaultDriverId BIGINT,
    FmVehicleId BIGINT,
    AdditionalMobileDevice VARCHAR(100),
    Notes TEXT,
    Icon VARCHAR(100),
    IconColour VARCHAR(50),
    Colour VARCHAR(50),
    AssetImage TEXT,
    IsDefaultImage BOOLEAN,
    AssetImageUrl TEXT,
    UserState VARCHAR(100),
    CreatedBy VARCHAR(100),
    CreatedDate DATETIME,
    Odometer FLOAT,
    EngineHours VARCHAR(20),
    Country VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS trips (
    TripId BIGINT PRIMARY KEY,
    AssetId BIGINT,
    DriverId BIGINT,
    TripStart DATETIME,
    TripEnd DATETIME,
    Notes TEXT,
    EngineSeconds BIGINT,
    FirstDepart DATETIME,
    LastHalt DATETIME,
    DrivingTime INT,
    StandingTime INT,
    Duration INT,
    DistanceKilometers FLOAT,
    StartOdometerKilometers FLOAT,
    EndOdometerKilometers FLOAT,
    StartEngineSeconds BIGINT,
    EndEngineSeconds BIGINT,
    PulseValue FLOAT,
    FuelUsedLitres FLOAT,
    MaxSpeedKilometersPerHour INT,
    MaxAccelerationKilometersPerHourPerSecond FLOAT,
    MaxDecelerationKilometersPerHourPerSecond FLOAT,
    MaxRpm INT
);
CREATE TABLE IF NOT EXISTS subtrips (
    id INT AUTO_INCREMENT PRIMARY KEY,
    TripId BIGINT,
    AssetId BIGINT,
    DriverId BIGINT,
    SubTripStart DATETIME,
    SubTripEnd DATETIME,
    StartOdometer FLOAT,
    EndOdometer FLOAT,
    Distance FLOAT,
    FuelUsed FLOAT,
    StartLatitude DECIMAL(10,8),
    StartLongitude DECIMAL(11,8),
    EndLatitude DECIMAL(10,8),
    EndLongitude DECIMAL(11,8)
);
