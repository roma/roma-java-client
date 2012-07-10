package jp.co.rakuten.rit.roma.client.util.commands;

import jp.co.rakuten.rit.roma.client.commands.CommandID;

/**
 * 
 */
public interface MapcountCommandID extends CommandID {

    // storage command
    int MAPCOUNT_GET = 150;

    int MAPCOUNT_COUNTUP = 151;

    int MAPCOUNT_UPDATE = 152;

    String STR_MAPCOUNT_GET = "mapcount_get";

    String STR_MAPCOUNT_COUNTUP = "mapcount_countup";

    String STR_MAPCOUNT_UPDATE = "mapcount_update";
    
    String STR_MAPCOUNT_FLAG = "0";
}
