package com.epam.bdcc.htm;

import java.io.Serializable;
import java.util.Map;

public class ResultState implements Serializable {
    private static final long serialVersionUID = 1L;

    private double actual = 0.0d;
    private double anomaly = 0.0d;
    private double error = 0.0d;
    private double prediction = 0.0d;
    private double predictionNext = 0.0d;
    private int recordNumber = -1;
    private Map<String, Object> inputMap;

    public ResultState() {
    }

    ResultState(int recordNumber, Map<String, Object> inputMap, double actual, double prediction, double error, double anomaly, double predictionNext) {
        this.recordNumber = recordNumber;
        this.inputMap = inputMap;
        this.actual = actual;
        this.prediction = prediction;
        this.error = error;
        this.anomaly = anomaly;
        this.predictionNext = predictionNext;
    }

    public void setState(int recordNumber, Map<String, Object> inputMap, double actual, double prediction, double error, double anomaly, double predictionNext) {
        this.recordNumber = recordNumber;
        this.inputMap = inputMap;
        this.actual = actual;
        this.prediction = prediction;
        this.error = error;
        this.anomaly = anomaly;
        this.predictionNext = predictionNext;
    }

    public double getActual() {
        return actual;
    }

    public void setActual(double actual) {
        this.actual = actual;
    }

    public double getAnomaly() {
        return anomaly;
    }

    public void setAnomaly(double anomaly) {
        this.anomaly = anomaly;
    }

    public double getError() {
        return error;
    }

    public void setError(double error) {
        this.error = error;
    }

    public double getPrediction() {
        return prediction;
    }

    public void setPrediction(double prediction) {
        this.prediction = prediction;
    }

    public double getPredictionNext() {
        return predictionNext;
    }

    public void setPredictionNext(double predictionNext) {
        this.predictionNext = predictionNext;
    }

    public int getRecordNumber() {
        return recordNumber;
    }

    public void setRecordNumber(int recordNumber) {
        this.recordNumber = recordNumber;
    }

    public Map<String, Object> getInputMap() {
        return inputMap;
    }

    public void setInputMap(Map<String, Object> inputMap) {
        this.inputMap = inputMap;
    }
}
