#include "vvarchar.h"
#include "executor/vtype.h"

PG_FUNCTION_INFO_V1(vvarchar_in);
PG_FUNCTION_INFO_V1(vvarchar_out);

PG_FUNCTION_INFO_V1(vvarcharin);
Datum vvarcharin(PG_FUNCTION_ARGS)
{
	    elog(ERROR, "vvarchar_in not supported");
}

PG_FUNCTION_INFO_V1(vvarcharout);
Datum vvarcharout(PG_FUNCTION_ARGS)
{
	    elog(ERROR, "vvarchar_out not supported");
}
