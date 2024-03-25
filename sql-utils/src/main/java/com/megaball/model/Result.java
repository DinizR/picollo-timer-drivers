package com.megaball.model;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
@Builder
public class Result {
   private Long id;
   private Long gameId;
   private LocalDate date;
   private Integer numYear;
   private String termination;
   private String numbers;
   private String stars;
   private String numbersPairOdd;
   private String starsPairOdd;
   private String decimalNumbers;
   private String decimalStars;
   private LocalDateTime createdStamp;
}
