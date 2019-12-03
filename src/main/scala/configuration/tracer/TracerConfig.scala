package configuration.tracer

case class TracerConfig(sourceDirectory: String,
                        destinationDirectory: String,
                        tracePath: String,
                        traceFileName: String,
                        traceMethod :String
                 )
