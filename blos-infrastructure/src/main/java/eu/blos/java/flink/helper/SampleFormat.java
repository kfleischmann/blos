package eu.blos.java.flink.helper;

import java.io.Serializable;

public class SampleFormat implements Serializable {
	String fieldDelimiter;
	String featureDelimiter;
	int FeaturesPosition;
	int LabelPosition;

	public SampleFormat(String fieldDelimiter, String featureDelimiter, int LabelPosition, int FeaturesPosition ){
		this.fieldDelimiter = fieldDelimiter;
		this.featureDelimiter = featureDelimiter;
		this.FeaturesPosition = FeaturesPosition;
		this.LabelPosition = LabelPosition;
	}

	public int getFeaturesPosition() {
		return FeaturesPosition;
	}

	public int getLabelPosition() {
		return LabelPosition;
	}

	public String getFeatureDelimiter() {
		return featureDelimiter;
	}

	public String getFieldDelimiter() {
		return fieldDelimiter;
	}
}
