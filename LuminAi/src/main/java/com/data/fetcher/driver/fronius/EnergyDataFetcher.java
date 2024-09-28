package com.data.fetcher.driver.fronius;

import com.data.model.Group;
import com.data.model.Sensor;
import com.data.model.SensorData;
import com.data.model.SensorDataId;
import com.data.spi.FetcherType;
import com.data.spi.ServiceInterface;
import com.data.utils.PropLoader;
import com.data.utils.Store;
import com.google.gson.Gson;
import io.quarkus.logging.Log;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class EnergyDataFetcher implements ServiceInterface {

    private Store store;
    private Properties properties;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private static String fetchDataFromApi(String apiUrl) throws IOException {
        URL url = new URL(apiUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");

        int responseCode = connection.getResponseCode();

        if (responseCode == HttpURLConnection.HTTP_OK) {
            Scanner scanner = new Scanner(url.openStream());
            StringBuilder response = new StringBuilder();
            while (scanner.hasNext()) {
                response.append(scanner.nextLine());
            }
            scanner.close();
            return response.toString();
        } else {
            throw new IOException("Failed to fetch data from API. Response code: " + responseCode);
        }
    }

    public /*List<SensorData>*/ void runDriver() throws RuntimeException {
        String apiUrl = properties.getProperty("apiUrl");
        String dataJson;

        try {
            dataJson = fetchDataFromApi(apiUrl);
        } catch (IOException e) {
            Log.info("Failed when fetching data from API! Maybe on wrong Network? :) Please Check if you can reach: " + apiUrl);
            throw new RuntimeException("Failed when fetching data from API: " + e);
        }

        Gson gson = new Gson();
        Map<String, Object> data = gson.fromJson(dataJson, Map.class);

        Map<String, Object> body = (Map<String, Object>) data.get("Body");
        Map<String, Object> dataProperty = (Map<String, Object>) body.get("Data");
        Map<String, Object> site = (Map<String, Object>) dataProperty.get("Site");

        Map<String, Object> head = (Map<String, Object>) data.get("Head");

        List<SensorData> dataList = new ArrayList<>();

        // innit Group
        Group fronius = new Group("Fronius");

        // Create or Get ALL Sensors and set Group
        Sensor pvSensor = new Sensor("PV");
        Sensor gridSensor = new Sensor("Grid");
        Sensor akkuSensor = new Sensor("Akku");


        //TODO:ob do oda so wiad nix mehr ausgeführt ka numoi schaun von wo genau

        //TODO:
        pvSensor.setGroup(fronius);
        gridSensor.setGroup(fronius);
        akkuSensor.setGroup(fronius);

        String dateString = (String) head.get("Timestamp");

        // Parse the string to OffsetDateTime
        OffsetDateTime offsetDateTime = OffsetDateTime.parse(dateString, DateTimeFormatter.ISO_OFFSET_DATE_TIME);

        // Convert OffsetDateTime to long timestamp
        long timestamp = offsetDateTime.toInstant().toEpochMilli();

        // PV
        try {
            if((Double) site.get("P_PV") == null) throw new RuntimeException();
            SensorDataId pvSensorDataId = new SensorDataId();
            pvSensorDataId.setTimestamp(timestamp);

            SensorData pvData = new SensorData();
            pvData.setSensorDataId(pvSensorDataId);
            pvData.setSensor(pvSensor);

            pvData.setValue((Double) site.get("P_PV"));
            dataList.add(pvData);
        } catch (Exception exception) {
            Log.info("There is no PV-Data in this Fronius API!");
        }
        // Grid
        try {
            if((Double) site.get("P_Grid") == null) throw new RuntimeException();
            SensorDataId gridSensorDataId = new SensorDataId();
            gridSensorDataId.setTimestamp((timestamp));

            SensorData gridData = new SensorData();
            gridData.setSensorDataId(gridSensorDataId);
            gridData.setSensor(gridSensor);

            gridData.setValue((Double) site.get("P_Grid"));
            dataList.add(gridData);
        } catch (Exception exception) {
            Log.info("There is no Grid-Data in this Fronius API!");
        }

        //Akku
        try {
            if((Double) site.get("P_Akku") == null) throw new RuntimeException();
            SensorDataId akkuSensorDataId = new SensorDataId();
            akkuSensorDataId.setTimestamp((timestamp));

            SensorData akkuData = new SensorData();
            akkuData.setSensorDataId(akkuSensorDataId);
            akkuData.setSensor(akkuSensor);

            akkuData.setValue((Double) site.get("P_Akku"));
            dataList.add(akkuData);
        } catch (Exception exception) {
            Log.info("There is no Akku-Data in this Fronius API!");
        }
        //TODO: try catch in loop
        try {
            Map<String, Object> secondaryMeters = (Map<String, Object>) (dataProperty.get("SecondaryMeters"));
            for (Map.Entry<String, Object> entry : secondaryMeters.entrySet()) {
                Map<String, Object> entryData = (Map<String, Object>) entry.getValue();

                //Create or Get Sensor
                Sensor sensor = new Sensor(String.valueOf(entryData.get("Label")));
                sensor.setGroup(fronius);

                SensorDataId sensorDataId = new SensorDataId();
                sensorDataId.setTimestamp((timestamp));

                SensorData sensorData = new SensorData();
                sensorData.setSensorDataId(sensorDataId);
                sensorData.setSensor(sensor);
                sensorData.setValue((Double) entryData.get("P"));

                //set connections
                dataList.add(sensorData);
            }
        } catch (Exception e){
            Log.info("Secondary Meters does not exist in this Fronius API: " + e);
        }
        store.getSubjectList().onNext(dataList);
        store.addAll();
    }

    @Override
    public void setStore(Store store) {
        this.store = store;
    }

    @Override
    public void setProperties() {
        PropLoader propLoader = new PropLoader();
        propLoader.setType(this.getType());
        this.properties = propLoader.getProperties();
    }

    @Override
    public CompletableFuture<Void> invoke() {
        if (properties != null) {
            return CompletableFuture.runAsync(() -> {
                try {
                    scheduler.scheduleAtFixedRate(this::runDriver, 0, 2, TimeUnit.SECONDS);
                } catch (RuntimeException e) {
                    Log.info("OIS OASCH");
                    throw new RuntimeException(e);
                }
            });
        }
        return null;
    }

    @Override
    public boolean stopService() {
        scheduler.shutdownNow();
        return scheduler.isShutdown();
    }

    @Override
    public FetcherType getType() {
        return FetcherType.DRIVER;
    }
}
