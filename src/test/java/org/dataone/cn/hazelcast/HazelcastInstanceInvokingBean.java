/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.hazelcast;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 *
 * @author waltz
 */
public class HazelcastInstanceInvokingBean {

    private static HazelcastInstance hazelcast = null;

    static {
        if (hazelcast == null) {
            ClasspathXmlConfig config = new ClasspathXmlConfig("org/dataone/configuration/hazelcastTestClientConf.xml");
            hazelcast = Hazelcast.newHazelcastInstance(config);
        }
    }

    public static HazelcastInstance getHazelcast() {
        return hazelcast;
    }

}
