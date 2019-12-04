package configuration.validator.nullHandler

case class NullValidatorConfig(mode: String,
                               columns: List[String]=null,
                               minNonNulls : Int= null,
                               how : String = null
                 )
//TODO check attributes