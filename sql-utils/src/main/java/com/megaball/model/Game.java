package com.megaball.model;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDate;
import java.util.List;

@Data
@Builder
public class Game {
   private Long id;
   private String description;
   private int numbers;
   private int extraNumbers;
}
