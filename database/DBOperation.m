

#import "DBOperation.h"
static sqlite3 *database = nil;
static int conn;
static NSString *db_name;
@implementation DBOperation

//Open database
+ (void) OpenDatabase:(NSString*)path
{
    

	@try
	{
		conn = sqlite3_open([path UTF8String], &database);
		if (conn == SQLITE_OK) {
			DLog(@"Database Open Successfully.");
		}
		else
			sqlite3_close(database); //Even though the open call failed, close the database connection to release all the memory.
	}	
	@catch (NSException *e) {

	}	
}



+(NSMutableArray*) selectData:(NSString *)sql{
	DLog(@"%@" , sql);
	if ( conn == SQLITE_OK) {

        
        sqlite3_stmt *stmt = nil;
        if(sqlite3_prepare_v2(database, [sql UTF8String], -1, &stmt, NULL) != SQLITE_OK) {
            [NSException raise:@"DatabaseException" format:@"Error while creating statement. '%s'", sqlite3_errmsg(database)];
        }
        NSMutableArray *obj = [[NSMutableArray alloc]init];
        int numResultColumns = 0;
        
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            NSMutableDictionary *tmpObj = [[NSMutableDictionary alloc]init];
            numResultColumns = sqlite3_column_count(stmt);
        
            for(int i = 0; i < numResultColumns; i++){
                if(sqlite3_column_type(stmt, i) == SQLITE_INTEGER){
                    
                    const char *name = sqlite3_column_name(stmt, i);
              ;
                    NSString *columnName = [NSString stringWithCString:name encoding:NSUTF8StringEncoding];
                    [tmpObj setObject:[NSString stringWithFormat:@"%i",sqlite3_column_int(stmt, i)] forKey:columnName];
                    
                } else if (sqlite3_column_type(stmt, i) == SQLITE_FLOAT) {
                    
                    const char *name = sqlite3_column_name(stmt, i);
                    NSString *columnName = [NSString stringWithCString:name encoding:NSUTF8StringEncoding];
       
                    [tmpObj setObject:[NSString stringWithFormat:@"%f",sqlite3_column_double(stmt, i)] forKey:columnName];
                } else if (sqlite3_column_type(stmt, i) == SQLITE_TEXT) {
                    const char *name = sqlite3_column_name(stmt, i);
                    NSString *tmpStr = [NSString stringWithUTF8String:(char *)sqlite3_column_text(stmt, i)];
                    if ( tmpStr == nil) {
                        tmpStr = @"";
                    }
                    NSString *columnName = [[NSString alloc]initWithCString:name encoding:NSUTF8StringEncoding];
                    [tmpObj setObject:tmpStr forKey:columnName];

                    
                } else if (sqlite3_column_type(stmt, i) == SQLITE_BLOB) {
                    
                                  }     
                
            }
            [obj addObject:tmpObj];
                }
  
        return obj;
	} else {
		return nil;
	}
}



+(BOOL) executeSQL:(NSString *)sqlTmp {
    DLog(@"------------\n Query >> %@" , sqlTmp);
    if(conn == SQLITE_OK) {
      
		
        const char *sqlStmt = [sqlTmp cStringUsingEncoding:NSUTF8StringEncoding];
        sqlite3_stmt *cmp_sqlStmt1;
        int returnValue = sqlite3_prepare_v2(database, sqlStmt, -1, &cmp_sqlStmt1, NULL);
		

		
        if (returnValue == SQLITE_OK) {
                DLog(@"\ninserted succefully\n-------------- ");
        }else{
                DLog(@"\n not ninserted\n-------------- ");        
        }
        
        sqlite3_step(cmp_sqlStmt1);
        sqlite3_finalize(cmp_sqlStmt1);
		
        if (returnValue == SQLITE_OK) {
            return TRUE;
        }
    }
    return FALSE;
}

+(int) getLastInsertId{
	return sqlite3_last_insert_rowid(database);
}

//Save data at application closing time
+ (void) finalizeStatements {
	
	if(database) sqlite3_close(database);
    
}

+(void)setDBName:(NSString *)database_name{
	db_name = database_name;
	[DBOperation copyDatabaseIfNeeded];
	NSString *path =[DBOperation getDBPath];
	[DBOperation OpenDatabase:path];
} 

+(void)copyDatabaseIfNeeded
{
	NSFileManager *filemanager=[NSFileManager defaultManager];
	NSError *error;
	NSString *dbPath =[DBOperation getDBPath];
	BOOL success =[filemanager fileExistsAtPath:dbPath];
	
	if(!success){
		NSString *defaultDBPath =[[[NSBundle mainBundle]resourcePath]stringByAppendingPathComponent:db_name];
		success = [filemanager copyItemAtPath:defaultDBPath toPath:dbPath error:&error];
		
		if(!success){
			NSAssert1(0,@"Failed to create writable databaase file with message '%@'.",[error localizedDescription]);	
		}
	}
}

+(NSMutableDictionary*)tableInfo:(NSString *)table
{
    sqlite3_stmt *sqlStatement;
    NSMutableDictionary *result = [[NSMutableDictionary alloc] init];
    const char *sql = [[NSString stringWithFormat:@"pragma table_info('%s')",[table UTF8String]] UTF8String];
    if(sqlite3_prepare(database, sql, -1, &sqlStatement, NULL) != SQLITE_OK)
    {
        DLog(@"Problem with prepare statement tableInfo %@",[NSString stringWithUTF8String:(const char *)sqlite3_errmsg(database)]);
        
    }
    while (sqlite3_step(sqlStatement)==SQLITE_ROW)
    {
        [result setObject:@"" forKey:[NSString stringWithUTF8String:(char*)sqlite3_column_text(sqlStatement, 1)]];
        
    }
    
    return result;
}

//Get database path
+(NSString *)getDBPath {
    
    NSString *documentsDir = [NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES) objectAtIndex:0];
	documentsDir = [documentsDir stringByAppendingPathComponent:db_name];
	return documentsDir;	
}



@end
