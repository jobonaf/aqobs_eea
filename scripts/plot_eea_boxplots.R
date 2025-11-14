#!/usr/bin/env Rscript
#
# title           : plot_eea_boxplots
# description     : Plot annual boxplots for EEA air quality data
# author          : Giovanni Bonafè | ARPA-FVG
# date            : 2025-11-04
# version         : 1.5
# usage           : Rscript plot_eea_boxplots.R -h
# notes           : Requires ggplot2, data.table, futile.logger, optparse, glue
#============================================================================

suppressPackageStartupMessages({
  library(optparse)
  library(futile.logger)
  library(ggplot2)
  library(data.table)
  library(glue)
})

# Command line options
option_list <- list(
  make_option(c("-i", "--input"), type = "character", help = "Input CSV file [required]"),
  make_option(c("-o", "--output"), type = "character", default = "eea_boxplots.pdf", help = "Output PDF [default: %default]"),
  make_option(c("-p", "--pollutants"), type = "character", help = "Pollutants to plot (e.g., PM10,NO2,O3)"),
  make_option(c("-v", "--verbose"), action = "store_true", default = FALSE, help = "Enable verbose output")
)

parser <- OptionParser(usage = "%prog -i input.csv [options]", option_list = option_list)
opt <- parse_args(parser)

if (is.null(opt$input)) stop("Input file is required")

# Setup
if (opt$verbose) flog.threshold(DEBUG) else flog.threshold(INFO)
flog.appender(appender.console())

# Load data
flog.info("Loading %s", opt$input)
data <- fread(opt$input)
flog.info("Loaded %s records", format(nrow(data), big.mark = ","))

# Filter pollutants
if (!is.null(opt$pollutants)) {
  pollutants <- strsplit(opt$pollutants, ",")[[1]]
  data <- data[Pollutant_Name %in% pollutants | Pollutant_Code %in% pollutants]
  flog.info("Filtered to %s records", format(nrow(data), big.mark = ","))
}

# Remove invalid and missing data
data <- data[Validity > 0 & !is.na(Value) & !is.na(Start)]
data[, Year := year(Start)]

# Calculate global year range across all data
all_years <- sort(unique(data$Year))
flog.info("Global year range: %s-%s", min(all_years), max(all_years))

# Create PDF
pdf(opt$output, width = 10, height = 6)
plotted <- 0

# Unique station-pollutant combinations
combinations <- unique(data[, .(Station_clean, Pollutant_Code)])

for (i in 1:nrow(combinations)) {
  combo <- combinations[i]
  station_data <- data[Station_clean == combo$Station_clean &
                         Pollutant_Code == combo$Pollutant_Code]
  
  if (nrow(station_data) < 10) next
  
  # Get years with data for this station
  station_years <- sort(unique(station_data$Year))
  
  if (length(station_years) < 2) {
    if (opt$verbose) flog.info("Skipping %s: only 1 year of data", combo$Station_clean)
    next
  }
  
  # Ensure all years in global range are represented using ordered factor
  station_data$Year_ordered <- ordered(station_data$Year, levels = all_years)
  station_data <- station_data[!is.na(Year_ordered)]
  
  title_txt <- glue("{station_data$`Air Quality Station Name`[1]} ({station_data$`Air Quality Station EoI Code`[1]})")
  subtitle_txt <- glue("{station_data$Pollutant_Name[1]} ({station_data$Pollutant_Code[1]})")
  y_label <- glue("Concentration ({station_data$Unit[1]})")
  caption_txt <- glue("Total records: {nrow(station_data)} | Years: {min(station_years)}-{max(station_years)} | Station: {station_data$Station_clean[1]}")
  
  p <- ggplot(station_data, aes(x = Year_ordered, y = Value)) +
    geom_boxplot(fill = "lightblue", alpha = 0.7, outlier.shape = NA) +
    scale_x_discrete(drop = FALSE) +  # Forza tutti i livelli nell'asse x
    labs(
      title = title_txt,
      subtitle = subtitle_txt,
      x = "Year",
      y = y_label,
      caption = caption_txt
    ) +
    theme_bw() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
  
  print(p)
  plotted <- plotted + 1
  if (opt$verbose) flog.info("Plotted %s: %s - %s (%s years)", i, 
                             station_data$`Air Quality Station Name`[1], 
                             station_data$Pollutant_Name[1],
                             length(station_years))
}

# Summary page
plot(0, 0, type = "n", xlim = c(0, 1), ylim = c(0, 1), axes = FALSE, xlab = "", ylab = "")
text(0.1, 0.9, "EEA Air Quality Boxplots", cex = 1.5, font = 2, pos = 4)
text(0.1, 0.8, glue("Plotted: {plotted} of {nrow(combinations)}"), cex = 1.2, pos = 4)
text(0.1, 0.7, glue("Pollutants: {paste(unique(data$Pollutant_Name), collapse = ', ')}"), cex = 1.1, pos = 4)
text(0.1, 0.6, glue("Stations: {length(unique(data$Station_clean))}"), cex = 1.1, pos = 4)
text(0.1, 0.5, glue("Year range: {min(all_years)}-{max(all_years)}"), cex = 1.1, pos = 4)
text(0.1, 0.4, glue("Generated: {Sys.time()}"), cex = 1.0, pos = 4)

dev.off()
flog.info("Created %s with %s boxplots", opt$output, plotted)