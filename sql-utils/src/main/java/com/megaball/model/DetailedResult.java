package com.megaball.model;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDate;
import java.util.List;

@Data
@Builder
public class DetailedResult {
   private Long id;
   private Long gameId;
   private LocalDate date;
   private List<Integer> numbers;
   private List<Integer> stars;
}
