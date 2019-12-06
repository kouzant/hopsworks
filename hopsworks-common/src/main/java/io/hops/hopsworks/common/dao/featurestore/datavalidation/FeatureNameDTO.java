package io.hops.hopsworks.common.dao.featurestore.datavalidation;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * This DTO is created only as a workaround to
 * https://bugs.eclipse.org/bugs/show_bug.cgi?id=459227
 * 
 * Returning GenericEntity<List<String>> doesn't work, so to overcome that the
 * String is wrapped in this DTO.
 *
 */
@XmlRootElement
public class FeatureNameDTO {

  private String featureName;

  public FeatureNameDTO() {
  }

  public FeatureNameDTO(String name) {
    this.featureName = name;
  }

  public String getFeatureName() {
    return featureName;
  }

  public void setFeatureName(String featureName) {
    this.featureName = featureName;
  }

}
