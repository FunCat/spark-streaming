package com.epam.bdcc.htm;

import java.io.Serializable;

public class MonitoringRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private String stateCode;
    private String countyCode;
    private String siteNum;
    private String parameterCode;
    private String poc;
    private String latitude;
    private String longitude;
    private String datum;
    private String parameterName;
    private String dateLocal;
    private String timeLocal;
    private String dateGMT;
    private String timeGMT;
    private String sampleMeasurement;
    private String unitsOfMeasure;
    private String mdl;
    private String uncertainty;
    private String qualifier;
    private String methodType;
    private String methodCode;
    private String methodName;
    private String stateName;
    private String countyName;
    private String dateOfLastChange;

    private double prediction;
    private double error;
    private double anomaly;
    private double predictionNext;

    public MonitoringRecord() {
    }

    public MonitoringRecord(String stateCode,
                            String countyCode,
                            String siteNum,
                            String parameterCode,
                            String poc,
                            String latitude,
                            String longitude,
                            String datum,
                            String parameterName,
                            String dateLocal,
                            String timeLocal,
                            String dateGMT,
                            String timeGMT,
                            String sampleMeasurement,
                            String unitsOfMeasure,
                            String mdl,
                            String uncertainty,
                            String qualifier,
                            String methodType,
                            String methodCode,
                            String methodName,
                            String stateName,
                            String countyName,
                            String dateOfLastChange,
                            double prediction,
                            double error,
                            double anomaly,
                            double predictionNext
    ) {
        this.stateCode = stateCode;
        this.countyCode = countyCode;
        this.siteNum = siteNum;
        this.parameterCode = parameterCode;
        this.poc = poc;
        this.latitude = latitude;
        this.longitude = longitude;
        this.datum = datum;
        this.parameterName = parameterName;
        this.dateLocal = dateLocal;
        this.timeLocal = timeLocal;
        this.dateGMT = dateGMT;
        this.timeGMT = timeGMT;
        this.sampleMeasurement = sampleMeasurement;
        this.unitsOfMeasure = unitsOfMeasure;
        this.mdl = mdl;
        this.uncertainty = uncertainty;
        this.qualifier = qualifier;
        this.methodType = methodType;
        this.methodCode = methodCode;
        this.methodName = methodName;
        this.stateName = stateName;
        this.countyName = countyName;
        this.dateOfLastChange = dateOfLastChange;

        this.prediction = prediction;
        this.error = error;
        this.anomaly = anomaly;
        this.predictionNext = predictionNext;
    }

    public MonitoringRecord(String[] line) {
        this.stateCode = line[0];
        this.countyCode = line[1];
        this.siteNum = line[2];
        this.parameterCode = line[3];
        this.poc = line[4];
        this.latitude = line[5];
        this.longitude = line[6];
        this.datum = line[7];
        this.parameterName = line[8];
        this.dateLocal = line[9];
        this.timeLocal = line[10];
        this.dateGMT = line[11];
        this.timeGMT = line[12];
        this.sampleMeasurement = line[13];
        this.unitsOfMeasure = line[14];
        this.mdl = line[15];
        this.uncertainty = line[16];
        this.qualifier = line[17];
        this.methodType = line[18];
        this.methodCode = line[19];
        this.methodName = line[20];
        this.stateName = line[21];
        this.countyName = line[22];
        this.dateOfLastChange = line[23];

        if (line.length == 28) {
            this.prediction = Double.parseDouble(line[24]);
            this.error = Double.parseDouble(line[25]);
            this.anomaly = Double.parseDouble(line[26]);
            this.predictionNext = Double.parseDouble(line[27]);
        } else {
            this.prediction = 0.0d;
            this.error = 0.0d;
            this.anomaly = 0.0d;
            this.predictionNext = 0.0d;
        }
    }

    public String getStateCode() {
        return stateCode;
    }

    public void setStateCode(String stateCode) {
        this.stateCode = stateCode;
    }

    public String getCountyCode() {
        return countyCode;
    }

    public void setCountyCode(String countyCode) {
        this.countyCode = countyCode;
    }

    public String getSiteNum() {
        return siteNum;
    }

    public void setSiteNum(String siteNum) {
        this.siteNum = siteNum;
    }

    public String getParameterCode() {
        return parameterCode;
    }

    public void setParameterCode(String parameterCode) {
        this.parameterCode = parameterCode;
    }

    public String getPoc() {
        return poc;
    }

    public void setPoc(String poc) {
        this.poc = poc;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getDatum() {
        return datum;
    }

    public void setDatum(String datum) {
        this.datum = datum;
    }

    public String getParameterName() {
        return parameterName;
    }

    public void setParameterName(String parameterName) {
        this.parameterName = parameterName;
    }

    public String getDateLocal() {
        return dateLocal;
    }

    public void setDateLocal(String dateLocal) {
        this.dateLocal = dateLocal;
    }

    public String getTimeLocal() {
        return timeLocal;
    }

    public void setTimeLocal(String timeLocal) {
        this.timeLocal = timeLocal;
    }

    public String getDateGMT() {
        return dateGMT;
    }

    public void setDateGMT(String dateGMT) {
        this.dateGMT = dateGMT;
    }

    public String getTimeGMT() {
        return timeGMT;
    }

    public void setTimeGMT(String timeGMT) {
        this.timeGMT = timeGMT;
    }

    public String getSampleMeasurement() {
        return sampleMeasurement;
    }

    public void setSampleMeasurement(String sampleMeasurement) {
        this.sampleMeasurement = sampleMeasurement;
    }

    public String getUnitsOfMeasure() {
        return unitsOfMeasure;
    }

    public void setUnitsOfMeasure(String unitsOfMeasure) {
        this.unitsOfMeasure = unitsOfMeasure;
    }

    public String getMdl() {
        return mdl;
    }

    public void setMdl(String mdl) {
        this.mdl = mdl;
    }

    public String getUncertainty() {
        return uncertainty;
    }

    public void setUncertainty(String uncertainty) {
        this.uncertainty = uncertainty;
    }

    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }

    public String getMethodType() {
        return methodType;
    }

    public void setMethodType(String methodType) {
        this.methodType = methodType;
    }

    public String getMethodCode() {
        return methodCode;
    }

    public void setMethodCode(String methodCode) {
        this.methodCode = methodCode;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public String getStateName() {
        return stateName;
    }

    public void setStateName(String stateName) {
        this.stateName = stateName;
    }

    public String getCountyName() {
        return countyName;
    }

    public void setCountyName(String countyName) {
        this.countyName = countyName;
    }

    public String getDateOfLastChange() {
        return dateOfLastChange;
    }

    public void setDateOfLastChange(String dateOfLastChange) {
        this.dateOfLastChange = dateOfLastChange;
    }

    public double getPrediction() {
        return prediction;
    }

    public void setPrediction(double prediction) {
        this.prediction = prediction;
    }

    public double getError() {
        return error;
    }

    public void setError(double error) {
        this.error = error;
    }

    public double getAnomaly() {
        return anomaly;
    }

    public void setAnomaly(double anomaly) {
        this.anomaly = anomaly;
    }

    public double getPredictionNext() {
        return predictionNext;
    }

    public void setPredictionNext(double predictionNext) {
        this.predictionNext = predictionNext;
    }

    @Override
    public String toString() {
        return "MonitoringRecord [stateCode=" + stateCode + ", countyCode=" + countyCode + ", siteNum=" + siteNum
                + ", parameterCode=" + parameterCode + ", poc=" + poc + ", latitude=" + latitude + ", longitude="
                + longitude + ", datum=" + datum + ", parameterName=" + parameterName + ", dateLocal=" + dateLocal
                + ", timeLocal=" + timeLocal + ", dateGMT=" + dateGMT + ", timeGMT=" + timeGMT + ", sampleMeasurement="
                + sampleMeasurement + ", unitsOfMeasure=" + unitsOfMeasure + ", mdl=" + mdl + ", uncertainty="
                + uncertainty + ", qualifier=" + qualifier + ", methodType=" + methodType + ", methodCode=" + methodCode
                + ", methodName=" + methodName + ", stateName=" + stateName + ", countyName=" + countyName
                + ", dateOfLastChange=" + dateOfLastChange + ", prediction=" + prediction + ", error=" + error
                + ", anomaly=" + anomaly + ", predictionNext=" + predictionNext + "]";
    }

}