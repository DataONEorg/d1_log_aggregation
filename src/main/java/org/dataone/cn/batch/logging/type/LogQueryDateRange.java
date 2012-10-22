/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.logging.type;

import java.util.Date;

/**
 * Provide a date range for queries to a logging endpoint
 * 
 * Used in conjuction with a stack to keep track of the branch of the query operation
 * 
 * @author waltz
 */
public class LogQueryDateRange {

    private Date fromDate;
    private Date toDate;

    public LogQueryDateRange(Date fromDate, Date toDate) {
        this.fromDate = fromDate;
        this.toDate = toDate;
    }

    public Date getFromDate() {
        return fromDate;
    }

    public Date getToDate() {
        return toDate;
    }
}
