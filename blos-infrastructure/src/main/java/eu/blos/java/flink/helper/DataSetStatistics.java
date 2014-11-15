package eu.blos.java.flink.helper;

public class DataSetStatistics {
	private int SampleCount;
	private int FeatureCount;
	private String[] labels;

	public DataSetStatistics(){
	}

	public void setSampleCount(int sampleCount) {
		SampleCount = sampleCount;
	}

	public int getSampleCount() {
		return SampleCount;
	}

	public void setLabels(String[] labels) {
		this.labels = labels;
	}

	public String[] getLabels() {
		return labels;
	}

	public void setFeatureCount(int featureCount) {
		FeatureCount = featureCount;
	}

	public int getFeatureCount() {
		return FeatureCount;
	}
}
