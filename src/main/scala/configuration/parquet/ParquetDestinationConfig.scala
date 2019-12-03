package configuration.parquet

case class ParquetDestinationConfig(partitionColumn: String,
                                    destinationDirectory: String
                               )