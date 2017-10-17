#include "Operations.h"

void Operations::Drop_Table(string relation_name)
{
	schema_manager.deleteRelation(relation_name);
}