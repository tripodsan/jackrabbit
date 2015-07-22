/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.core.security.user;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.core.RepositoryImpl;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.test.JUnitTest;

/**
 * Performance test for JCR-3892.
 */
public class MembershipCacheInvalidationTest extends JUnitTest {

    private static final String TEST_USER_PREFIX = "MembershipCacheTestUser-";
    private static final String TEST_GROUP_PREFIX = "MembershipCacheTestGroup-";
    private static final String REPO_HOME = new File("target",
            MembershipCacheInvalidationTest.class.getSimpleName()).getPath();

    private static final int NUM_USERS = 50;

    private static final int NUM_GROUPS = 20;

    private RepositoryImpl repo;

    private JackrabbitSession session;

    private UserManager userMgr;

    private MembershipCache cache;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        FileUtils.deleteDirectory(new File(REPO_HOME));
        //RepositoryConfig config = RepositoryConfig.create(getClass().getResourceAsStream("repository.xml"), REPO_HOME);
        RepositoryConfig config = RepositoryConfig.create(getClass().getResourceAsStream("repository-membersplit.xml"), REPO_HOME);
        repo = RepositoryImpl.create(config);
        session = createSession();
        userMgr = session.getUserManager();
        cache = ((UserManagerImpl) userMgr).getMembershipCache();
        boolean autoSave = userMgr.isAutoSave();
        userMgr.autoSave(false);
        // create test users and groups
        for (int i = 0; i < NUM_USERS; i++) {
            userMgr.createUser(TEST_USER_PREFIX + i, "secret");
        }
        for (int i = 0; i < NUM_GROUPS; i++) {
            userMgr.createGroup(TEST_GROUP_PREFIX + i);
        }
        session.save();
        userMgr.autoSave(autoSave);
        logger.info("Initial cache size: {}/{}", cache.getMembershipCacheSize(), cache.getMemberCacheSize());
    }

    @Override
    protected void tearDown() throws Exception {
        logger.info("end cache size: {}/{}", cache.getMembershipCacheSize(), cache.getMemberCacheSize());
        boolean autoSave = userMgr.isAutoSave();
        userMgr.autoSave(false);
        for (int i = 0; i < NUM_USERS; i++) {
            userMgr.getAuthorizable(TEST_USER_PREFIX + i).remove();
        }
        for (int i = 0; i < NUM_GROUPS; i++) {
            Authorizable a = userMgr.getAuthorizable(TEST_GROUP_PREFIX + i);
            if (a != null) {
                a.remove();
            }
        }
        session.save();
        userMgr.autoSave(autoSave);
        userMgr = null;
        cache = null;
        session.logout();
        repo.shutdown();
        repo = null;
        FileUtils.deleteDirectory(new File(REPO_HOME));
        super.tearDown();
    }

    private JackrabbitSession createSession() throws RepositoryException {
        return (JackrabbitSession) repo.login(
                new SimpleCredentials("admin", "admin".toCharArray()));
    }

    private void assertMembership(String id, int ... groups) throws RepositoryException {
        Authorizable a = userMgr.getAuthorizable(id);
        List<String> memberships = new ArrayList<String>();
        Iterator<Group> iter = a.memberOf();
        while (iter.hasNext()) {
            memberships.add(iter.next().getID());
        }
        Collections.sort(memberships);
        StringBuilder result = new StringBuilder();
        for (String groupId: memberships) {
            result.append(groupId).append("\n");
        }

        List<String> expectedMemberships = new ArrayList<String>();
        for (int i: groups) {
            if (i>=0) {
                expectedMemberships.add(TEST_GROUP_PREFIX + i);
            }
        }
        Collections.sort(expectedMemberships);
        StringBuilder expected = new StringBuilder();
        for (String groupId: expectedMemberships) {
            expected.append(groupId).append("\n");
        }

        assertEquals("membership", expected.toString(), result.toString());
    }

    private void addMembers(int groupIdx, String ... authId) throws RepositoryException {
        userMgr.autoSave(false);
        Group g = (Group) userMgr.getAuthorizable(TEST_GROUP_PREFIX + groupIdx);
        for (String id: authId) {
            Authorizable a = userMgr.getAuthorizable(id);
            g.addMember(a);
        }
        session.save();
    }

    private void removeMembers(int groupIdx, String ... authId) throws RepositoryException {
        userMgr.autoSave(false);
        Group g = (Group) userMgr.getAuthorizable(TEST_GROUP_PREFIX + groupIdx);
        for (String id: authId) {
            Authorizable a = userMgr.getAuthorizable(id);
            g.removeMember(a);
        }
        session.save();
    }

    public void testAddSingleMember() throws RepositoryException {
        assertMembership(TEST_USER_PREFIX + 0);

        addMembers(0, TEST_USER_PREFIX + 0);
        assertMembership(TEST_USER_PREFIX + 0, 0);
    }

    public void testRemoveSingleMember() throws RepositoryException {
        assertMembership(TEST_USER_PREFIX + 0);

        addMembers(0, TEST_USER_PREFIX + 0, TEST_USER_PREFIX + 1);
        assertMembership(TEST_USER_PREFIX + 0, 0);
        assertMembership(TEST_USER_PREFIX + 1, 0);

        removeMembers(0, TEST_USER_PREFIX + 1);
        assertMembership(TEST_USER_PREFIX + 0, 0);
        assertMembership(TEST_USER_PREFIX + 1);
    }

    public void testAddTransitive() throws RepositoryException {
        assertMembership(TEST_USER_PREFIX + 0);
        assertMembership(TEST_GROUP_PREFIX + 0);

        addMembers(0, TEST_USER_PREFIX + 0);
        addMembers(1, TEST_GROUP_PREFIX + 0);
        addMembers(2, TEST_GROUP_PREFIX + 1);
        addMembers(2, TEST_USER_PREFIX + 1);
        addMembers(2, TEST_USER_PREFIX + 2);

        assertMembership(TEST_USER_PREFIX + 0, 0, 1, 2);
    }

    public void testRemoveTransitive() throws RepositoryException {
        assertMembership(TEST_USER_PREFIX + 0);
        assertMembership(TEST_GROUP_PREFIX + 0);

        addMembers(0, TEST_USER_PREFIX + 0);
        addMembers(1, TEST_GROUP_PREFIX + 0);
        addMembers(2, TEST_GROUP_PREFIX + 1);
        addMembers(2, TEST_USER_PREFIX + 1);
        addMembers(2, TEST_USER_PREFIX + 2);

        assertMembership(TEST_USER_PREFIX + 0, 0, 1, 2);
        assertMembership(TEST_GROUP_PREFIX + 0, 1, 2);
        assertMembership(TEST_GROUP_PREFIX + 1, 2);

        // removing another user should keep the cache good
        int size = cache.getMembershipCacheSize();
        removeMembers(2, TEST_USER_PREFIX + 1);
        assertMembership(TEST_USER_PREFIX + 0, 0, 1, 2);

        assertEquals(size, cache.getMembershipCacheSize());

        // removing member of group 2 should recursively invalidate the rest
        removeMembers(2, TEST_GROUP_PREFIX + 1);
        assertMembership(TEST_USER_PREFIX + 0, 0, 1);
    }

    public void testLowMembershipCache() throws RepositoryException {
        cache.setMembershipCacheMaxSize(10);
        for (int i=0; i< NUM_USERS; i++) {
            assertMembership(TEST_USER_PREFIX + i);
        }
        testRemoveTransitive();
    }

    public void testLowMemberCache() throws RepositoryException {
        cache.setMemberCacheMaxSize(10);
        int[] groups = new int[NUM_GROUPS];
        for (int i=0; i< NUM_GROUPS; i++) {
            groups[i] = i;
            addMembers(i, TEST_USER_PREFIX + 4);
            assertMembership(TEST_GROUP_PREFIX + i);
        }
        assertMembership(TEST_USER_PREFIX + 4, groups);

        removeMembers(0, TEST_USER_PREFIX + 4);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        groups[0] = -1;
        assertMembership(TEST_USER_PREFIX + 4, groups);

        testRemoveTransitive();
    }

    public void testRemoveGroup() throws RepositoryException {
        assertMembership(TEST_USER_PREFIX + 0);

        addMembers(0, TEST_USER_PREFIX + 0, TEST_USER_PREFIX + 1);
        assertMembership(TEST_USER_PREFIX + 0, 0);
        assertMembership(TEST_USER_PREFIX + 1, 0);

        userMgr.autoSave(true);
        userMgr.getAuthorizable(TEST_GROUP_PREFIX + 0).remove();

        assertMembership(TEST_USER_PREFIX + 0);
    }

}
