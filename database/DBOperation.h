
#import <Foundation/Foundation.h>
#import <sqlite3.h>

@interface DBOperation : NSObject 
{
	
}

+(void)OpenDatabase:(NSString*)path;  //Open the Database
+(void)finalizeStatements;//Closing and do the final statement at application exits
+(int) getLastInsertId;
+(BOOL) executeSQL:(NSString *)sqlTmp;
+(NSMutableArray*) selectData:(NSString *)sql;
+(void)setDBName:(NSString *)database_name;
+(void)copyDatabaseIfNeeded;
+(NSString *)getDBPath ;
+(NSMutableDictionary*)tableInfo:(NSString *)table;
@end