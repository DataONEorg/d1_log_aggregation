/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.logging;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.dataone.client.auth.CertificateManager;
import org.dataone.service.types.v1.AccessRule;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.Constants;

/**
 * Get Access restrictions of Log Records
 * 
 * @author waltz
 */
public class LogAccessRestriction {

    private static Logger logger = Logger.getLogger(LogAccessRestriction.class.getName());
    private static Subject authenticatedSubject = new Subject();
    private static Subject verifiedSubject = new Subject();

    static {
        authenticatedSubject.setValue(Constants.SUBJECT_AUTHENTICATED_USER);
        verifiedSubject.setValue(Constants.SUBJECT_VERIFIED_USER);
    }
    /*
     * build a list of all subjects that are granted Change Permssion in
     * the systemMetadata of the object.  The rights holder is always
     * granted Change Permission.
     * 
     * @author waltz
     * @param SystemMetadata
     * @returns List<String> 
     */
    public List<String> subjectsAllowedRead(SystemMetadata systemMetadata) {
        List<String> subjectsAllowedRead = new ArrayList<String>();
        Subject rightsHolder = systemMetadata.getRightsHolder();
        if ((rightsHolder != null) && !(rightsHolder.getValue().isEmpty())) {
            try {
                String standardizedName = CertificateManager.getInstance().standardizeDN(rightsHolder.getValue());
                subjectsAllowedRead.add(standardizedName);
            } catch (IllegalArgumentException ex) {
                logger.warn("SystemMetadata with PID " + systemMetadata.getIdentifier().getValue() + " has a Subject: " + rightsHolder.getValue() + " that does not conform to RFC2253 conventions\n" + ex.getMessage());
                subjectsAllowedRead.add(rightsHolder.getValue());
            }
        }
        if (systemMetadata.getAccessPolicy() != null) {
            List<AccessRule> allowList = systemMetadata.getAccessPolicy().getAllowList();
            for (AccessRule accessRule : allowList) {
                List<Permission> permissionList = accessRule.getPermissionList();
                // only those with ChangePermission permission may read a log entry
                if (permissionList.contains(Permission.CHANGE_PERMISSION)) {
                    List<Subject> subjectList = accessRule.getSubjectList();
                    for (Subject accessSubject : subjectList) {
                        if (accessSubject.equals(authenticatedSubject)) {
                            subjectsAllowedRead.add(Constants.SUBJECT_AUTHENTICATED_USER);
                        } else if (accessSubject.equals(verifiedSubject)) {
                            subjectsAllowedRead.add(Constants.SUBJECT_VERIFIED_USER);
                        } else {
                            try {
                                // add subject as having read access on the record
                                String standardizedName = accessSubject.getValue();
                                try {
                                	standardizedName = CertificateManager.getInstance().standardizeDN(standardizedName);
                                } catch (IllegalArgumentException ex) {
                                	// non-DNs are acceptable
                                }
                                subjectsAllowedRead.add(standardizedName);
                            } catch (IllegalArgumentException ex) {
                                // It may be a group or as yet unidentified pseudo-user,  so just add the subject's value
                                // without attempting to standardize it
                                subjectsAllowedRead.add(accessSubject.getValue());
                                logger.warn("SystemMetadata with PID " + systemMetadata.getIdentifier().getValue() + " has a Subject: " + accessSubject.getValue() + " that does not conform to RFC2253 conventions");
                            }
                        }
                    }
                }
            }
        } else {
            logger.info("SystemMetadata with PID " + systemMetadata.getIdentifier().getValue() + " does not have an access policy");
        }
        return subjectsAllowedRead;
    }
}
