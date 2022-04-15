package com.wong.elastic.pojo;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

@Document(indexName = "item")
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Product implements Serializable {
  @Id
  private Long id;
  @Field(type = FieldType.Text,analyzer = "ik_max_word")
  private String title;
  @Field(type=FieldType.Keyword)
  private String brand;
  @Field(type=FieldType.Double)
  private Double price;
  @Field(index = false,type = FieldType.Keyword)
  private String images;
}
