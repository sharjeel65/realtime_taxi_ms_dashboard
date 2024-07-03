package bd24_project_a6_b.taxi_datastreaming.src.main.java.org.example;
package bd24_project_a6_b.taxi_datastreaming.src.main.java.Dto;

import Dto.Transaction;
import Dto.AverageSpeedDistance;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class SpeedDistanceCalculator extends KeyedProcessFunction<String, TaxiData, AverageSpeedDistance> {

    private final Map<String, TaxiData> tripStartData = new HashMap<>();

    @Override
    public void processElement(TaxiData taxiData, Context context, Collector<AverageSpeedDistance> collector) throws Exception {
        if (taxiData.isStarting()) {
            tripStartData.put(taxiData.getTaxiId(), taxiData);
        } else if (taxiData.isEnding()) {
            TaxiData startData = tripStartData.remove(taxiData.getTaxiId());
            if (startData != null) {
                double distance = calculateDistance(startData.getLatitude(), startData.getLongitude(), taxiData.getLatitude(), taxiData.getLongitude());
                long durationMillis = Duration.between(startData.getTimestamp().toLocalDateTime(), taxiData.getTimestamp().toLocalDateTime()).toMillis();
                double durationHours = durationMillis / (1000.0 * 60 * 60);
                double averageSpeed = distance / durationHours;
                collector.collect(new AverageSpeedDistance(taxiData.getTaxiID(), distance, averageSpeed));
            }
        }
    }

    private double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
        final int R = 6371; // Radius of the earth in km
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c; // Convert to km
    }
}