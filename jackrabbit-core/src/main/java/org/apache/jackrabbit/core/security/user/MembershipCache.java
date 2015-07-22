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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.jcr.AccessDeniedException;
import javax.jcr.ItemNotFoundException;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;

import org.apache.jackrabbit.commons.flat.BTreeManager;
import org.apache.jackrabbit.commons.flat.ItemSequence;
import org.apache.jackrabbit.commons.flat.PropertySequence;
import org.apache.jackrabbit.commons.flat.Rank;
import org.apache.jackrabbit.commons.flat.TreeManager;
import org.apache.jackrabbit.core.NodeImpl;
import org.apache.jackrabbit.core.PropertyImpl;
import org.apache.jackrabbit.core.SessionImpl;
import org.apache.jackrabbit.core.SessionListener;
import org.apache.jackrabbit.core.cache.ConcurrentCache;
import org.apache.jackrabbit.core.observation.SynchronousEventListener;
import org.apache.jackrabbit.spi.commons.iterator.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <code>MembershipCache</code> is used to load and cache the memberships of authorizables. The reverse lookup of
 * the groups an authorizable belongs to, can be slow on large systems, since the WEAK_REFERENCE resolution uses a query.
 *
 * The cache remembers the declared (direct) group ids of the authorizables in the {@code membership} cache.
 * It also remembers the members that were involved during a lookup in the {@code members} cache.
 *
 * When a group is modified, it checks if a member that was used during the lookup is no longer exits (remove member).
 * In this case the cache for that authorizable (and all decedents) is invalidated. If a member exists that we already
 * have a cached entry for, but was not used during lookup, we invalidate as well (add member).
 *
 * Please note, that if any of the caches are full, we cannot calculate the individual invalidation anymore, and the
 * entire cache is flushed.
 */
public class MembershipCache implements UserConstants, SynchronousEventListener, SessionListener {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(MembershipCache.class);

    /**
     * The maximum size of the membership cache
     */
    private static final int MAX_MEMBERSHIP_CACHE_SIZE =
            Integer.getInteger("org.apache.jackrabbit.MembershipCache", 5000);

    /**
     * The maximum size of this cache
     */
    private static final int MAX_MEMBER_CACHE_SIZE =
            Integer.getInteger("org.apache.jackrabbit.MemberCache", 5000);

    private final SessionImpl systemSession;
    private final String groupsPath;
    private final boolean useMembersNode;

    private final String pMembers;

    private final String repMembersSuffix;

    /**
     * Cache that contains the declared memberships of the authorizables
     * (key = authorizableNodeIdentifier, value = collection of groupNodeIdentifier)
     */
    private final ConcurrentCache<String, Collection<String>> membership;

    /**
     * Cache that contains the group members that were used during lookup.
     * (key = groupNodeIdentifier, value = collection of member nodeIdentifier)
     *
     * Note: we only define a map here, so we can use a {@link ConcurrentHashMap}
     */
    private final ConcurrentCache<String, Map<String,String>> members;


    MembershipCache(SessionImpl systemSession, String groupsPath, boolean useMembersNode) throws RepositoryException {
        this.systemSession = systemSession;
        this.groupsPath = (groupsPath == null) ? UserConstants.GROUPS_PATH : groupsPath;
        this.useMembersNode = useMembersNode;

        pMembers = systemSession.getJCRName(UserManagerImpl.P_MEMBERS);
        repMembersSuffix = "/" + pMembers;
        membership = new ConcurrentCache<String, Collection<String>>("MembershipCache", 16);
        membership.setMaxMemorySize(MAX_MEMBERSHIP_CACHE_SIZE);

        members = new ConcurrentCache<String, Map<String,String>>("MemberCache", 16);
        members.setMaxMemorySize(MAX_MEMBER_CACHE_SIZE);

        String[] ntNames = new String[] {
                systemSession.getJCRName(UserConstants.NT_REP_GROUP),
                systemSession.getJCRName(UserConstants.NT_REP_MEMBERS)
        };
        // register event listener to be informed about membership changes.
        systemSession.getWorkspace().getObservationManager().addEventListener(this,
                Event.PROPERTY_ADDED | Event.PROPERTY_CHANGED | Event.PROPERTY_REMOVED | Event.NODE_ADDED | Event.NODE_REMOVED,
                groupsPath,
                true,
                null,
                ntNames,
                false);
        // make sure the membership cache is informed if the system session is
        // logged out in order to stop listening to events.
        systemSession.addListener(this);
        log.debug("Membership cache initialized. Max membership size = {}, Max member size = {}", MAX_MEMBERSHIP_CACHE_SIZE, MAX_MEMBER_CACHE_SIZE);
    }


    //------------------------------------------------------< EventListener >---
    /**
     * @see javax.jcr.observation.EventListener#onEvent(javax.jcr.observation.EventIterator)
     */
    public void onEvent(EventIterator eventIterator) {
        // evaluate if the membership cache needs to be cleared;
        final long t0 = System.nanoTime();
        boolean clear = false;
        Set<String> groupPaths = new HashSet<String>();
        while (eventIterator.hasNext() && !clear) {
            Event ev = eventIterator.nextEvent();
            try {
                String path = ev.getPath();
                // event must happen at or below a rep:members item
                int idx = path.indexOf(repMembersSuffix);
                if (idx > 0) {
                    String groupPath = path.substring(0, idx);
                    boolean newGroup = groupPaths.add(groupPath);
                    if (newGroup && log.isDebugEnabled()) {
                        log.debug("event received for modified group: {}", groupPath);
                    }
                }
            } catch (RepositoryException e) {
                log.warn("error during processing observation event, clearing cache", e);
                // exception while processing the event -> clear the cache to be sure it isn't outdated.
                clear = true;
            }
        }

        int numInvalidated = 0;
        if (!clear) {
            try {
                // now go through all modified groups and get the current group memberships
                for (String groupPath: groupPaths) {
                    if (!systemSession.nodeExists(groupPath)) {
                        // group no longer exists, and we can't determine the node identifier -> clear
                        log.info("observation event for deleted group {}, clearing cache.", groupPath);
                        clear = true;
                        break;
                    }
                    Node groupNode = systemSession.getNode(groupPath);
                    String gNid = groupNode.getIdentifier();
                    Map<String,String> oldMembers = members.get(gNid);
                    Map<String,String> newMembers = null;

                    // create a copy
                    oldMembers = oldMembers == null ? null : new HashMap<String, String>(oldMembers);

                    Iterator<String> refs = getMembershipProvider((NodeImpl) groupNode).getDeclaredMemberReferences();
                    while (refs.hasNext()) {
                        String aNid = refs.next();
                        if (oldMembers != null && oldMembers.remove(aNid) != null) {
                            // authorizable was used during lookup.
                            if (membership.containsKey(aNid)) {
                                log.trace("member {} of {} not altered.", aNid, gNid);
                                if (newMembers == null) {
                                    newMembers = new ConcurrentHashMap<String, String>();
                                }
                                newMembers.put(aNid, aNid);
                            } else {
                                log.trace("member {} of {} not but authorizable no longer cached.", aNid, gNid);
                            }
                        } else {
                            // authorizable was not used yet, check if we cached it
                            if (membership.containsKey(aNid)) {
                                // so this is an addition
                                log.trace("authorizable {} added as member of {}", aNid, gNid);
                                numInvalidated += invalidate(aNid);
                            } else {
                                // all good. never used.
                                log.trace("member {} of {} not in cache at all", aNid, gNid);
                            }
                        }
                    }

                    if (oldMembers != null && !oldMembers.isEmpty()) {
                        for (String aNid: oldMembers.keySet()) {
                            // authorizable no longer member of
                            if (membership.containsKey(aNid)) {
                                log.trace("authorizable {} removed as member from {}.", aNid, gNid);
                                numInvalidated += invalidate(aNid);
                            } else {
                                log.trace("authorizable {} no longer member of {} but also no longer cached.", aNid, gNid);
                            }
                        }
                    }

                    if (newMembers == null) {
                        members.remove(gNid);
                    } else {
                        members.put(gNid, newMembers, 1);
                    }
                }
            } catch (RepositoryException e) {
                log.info("internal error during event evaluation. clearing cache.", e);
                clear = true;
            }
        }

        if (clear) {
            numInvalidated += getMembershipCacheSize();
            clear();
            log.debug("Membership cache cleared because of problems in observation event.");
        }

        if (log.isDebugEnabled()) {
            log.debug("event processing took {}us. invalidated {} memberships", (System.nanoTime() - t0) / 1000, numInvalidated);
        }

    }

    //----------------------------------------------------< SessionListener >---
    /**
     * @see SessionListener#loggingOut(org.apache.jackrabbit.core.SessionImpl)
     */
    public void loggingOut(SessionImpl session) {
        try {
            systemSession.getWorkspace().getObservationManager().removeEventListener(this);
        } catch (RepositoryException e) {
            log.error("Unexpected error: Failed to stop event listening of MembershipCache.", e);
        }

    }

    /**
     * @see SessionListener#loggedOut(org.apache.jackrabbit.core.SessionImpl)
     */
    public void loggedOut(SessionImpl session) {
        // nothing to do
    }

    //--------------------------------------------------------------------------
    /**
     * @param authorizableNodeIdentifier The identifier of the node representing
     * the authorizable to retrieve the declared membership for.
     * @return A collection of node identifiers of those group nodes the
     * authorizable in question is declared member of.
     * @throws RepositoryException If an error occurs.
     */
    Collection<String> getDeclaredMemberOf(String authorizableNodeIdentifier) throws RepositoryException {
        return declaredMemberOf(authorizableNodeIdentifier);
    }

    /**
     * @param authorizableNodeIdentifier The identifier of the node representing
     * the authorizable to retrieve the membership for.
     * @return A collection of node identifiers of those group nodes the
     * authorizable in question is a direct or indirect member of.
     * @throws RepositoryException If an error occurs.
     */
    Collection<String> getMemberOf(String authorizableNodeIdentifier) throws RepositoryException {
        Set<String> groupNodeIds = new HashSet<String>();
        memberOf(authorizableNodeIdentifier, groupNodeIds);
        return Collections.unmodifiableCollection(groupNodeIds);
    }

    /**
     * Returns the size of the membership cache
     * @return the size
     */
    public int getMembershipCacheSize() {
        return (int) membership.getElementCount();
    }

    /**
     * Returns the size of the member cache
     * @return the size
     */
    public int getMemberCacheSize() {
        return (int) members.getElementCount();
    }

    /**
     * clears the cache
     */
    public void clear() {
        members.clear();
        membership.clear();
    }

    /**
     * invalidate a single entry
     * @param authorizableNodeId the node identifier to invalidate
     * @return the number of invalidated memberships
     */
    public int invalidate(String authorizableNodeId) {
        int num = membership.remove(authorizableNodeId) == null ? 0 : 1;
        if (log.isTraceEnabled()) {
            log.trace("invalidating {} (was cached={})", authorizableNodeId, num > 0);
        }
        Map<String, String> m = members.remove(authorizableNodeId);
        if (m != null) {
            for (String mnid: m.keySet()) {
                num += invalidate(mnid);
            }
        }
        return num;
    }

    /**
     * Sets the membership cache size
     * @param size the new size
     */
    public void setMembershipCacheMaxSize(int size) {
        clear();
        membership.setMaxMemorySize(size);
    }

    /**
     * Sets the member cache size
     * @param size the new size
     */
    public void setMemberCacheMaxSize(int size) {
        clear();
        members.setMaxMemorySize(size);
    }

    /**
     * Collects the declared memberships for the specified identifier of an
     * authorizable using the specified session.
     * 
     * @param authorizableNodeIdentifier The identifier of the node representing
     * the authorizable to retrieve the membership for.
     * @param session The session to be used to read the membership information.
     * @return @return A collection of node identifiers of those group nodes the
     * authorizable in question is a direct member of.
     * @throws RepositoryException If an error occurs.
     */
    Collection<String> collectDeclaredMembership(String authorizableNodeIdentifier, Session session) throws RepositoryException {
        final long t0 = System.nanoTime();

        Collection<String> groupNodeIds = collectDeclaredMembershipFromReferences(authorizableNodeIdentifier, session);

        final long t1 = System.nanoTime();
        if (log.isDebugEnabled()) {
            log.debug("  collected {} groups for {} via references in {}us", new Object[]{
                    groupNodeIds == null ? -1 : groupNodeIds.size(),
                    authorizableNodeIdentifier,
                    (t1-t0) / 1000
            });
        }

        if (groupNodeIds == null) {
            groupNodeIds = collectDeclaredMembershipFromTraversal(authorizableNodeIdentifier, session);

            final long t2 = System.nanoTime();
            if (log.isDebugEnabled()) {
                log.debug("  collected {} groups for {} via traversal in {}us", new Object[]{
                        groupNodeIds == null ? -1 : groupNodeIds.size(),
                        authorizableNodeIdentifier,
                        (t2-t1) / 1000
                });
            }
        }
        return groupNodeIds;
    }

    /**
     * Collects the complete memberships for the specified identifier of an
     * authorizable using the specified session.
     *
     * @param authorizableNodeIdentifier The identifier of the node representing
     * the authorizable to retrieve the membership for.
     * @param session The session to be used to read the membership information.
     * @return A collection of node identifiers of those group nodes the
     * authorizable in question is a direct or indirect member of.
     * @throws RepositoryException If an error occurs.
     */
    Collection<String> collectMembership(String authorizableNodeIdentifier, Session session) throws RepositoryException {
        Set<String> groupNodeIds = new HashSet<String>();
        memberOf(authorizableNodeIdentifier, groupNodeIds, session);
        return groupNodeIds;
    }

    //------------------------------------------------------------< private >---
    /**
     * Collects the groups where the given authorizable is a declared member of. If the information is not cached, it
     * is collected from the repository.
     *
     * @param authorizableNodeIdentifier Identifier of the authorizable node
     * @return the collection of groups where the authorizable is a declared member of
     * @throws RepositoryException if an error occurs
     */
    private Collection<String> declaredMemberOf(String authorizableNodeIdentifier) throws RepositoryException {
        final long t0 = System.nanoTime();

        Collection<String> groupNodeIds = membership.get(authorizableNodeIdentifier);

        boolean wasCached = true;
        if (groupNodeIds == null) {
            wasCached = false;
            // retrieve a new session with system-subject in order to avoid
            // concurrent read operations using the system session of this workspace.
            Session session = getSession();
            try {
                groupNodeIds = collectDeclaredMembership(authorizableNodeIdentifier, session);
                membership.put(authorizableNodeIdentifier, Collections.unmodifiableCollection(groupNodeIds), 1);
                for (String groupId: groupNodeIds) {
                    Map<String, String> m = members.get(groupId);
                    if (m == null) {
                        m = new ConcurrentHashMap<String, String>();
                        members.put(groupId, m, 1);
                    }
                    m.put(authorizableNodeIdentifier, authorizableNodeIdentifier);
                }
            }
            finally {
                // release session if it isn't the original system session
                if (session != systemSession) {
                    session.logout();
                }
            }
        }

        if (log.isDebugEnabled()) {
            final long t1 = System.nanoTime();
            log.debug("Membership cache {} {} declared memberships of {} in {}us. cache size = {}/{}", new Object[]{
                    wasCached ? "returns" : "collected",
                    groupNodeIds.size(),
                    authorizableNodeIdentifier,
                    (t1-t0) / 1000,
                    getMembershipCacheSize(),
                    getMemberCacheSize()
            });
        }
        return groupNodeIds;
    }

    /**
     * Collects the groups where the given authorizable is a member of by recursively fetching the declared memberships
     * via {@link #declaredMemberOf(String)} (cached).
     *
     * @param authorizableNodeIdentifier Identifier of the authorizable node
     * @param groupNodeIds Map to receive the node ids of the groups
     * @throws RepositoryException if an error occurs
     */
    private void memberOf(String authorizableNodeIdentifier, Collection<String> groupNodeIds) throws RepositoryException {
        Collection<String> declared = declaredMemberOf(authorizableNodeIdentifier);
        for (String identifier : declared) {
            if (groupNodeIds.add(identifier)) {
                memberOf(identifier, groupNodeIds);
            }
        }
    }

    /**
     * Collects the groups where the given authorizable is a member of by recursively fetching the declared memberships
     * by reading the relations from the repository (uncached!).
     *
     * @param authorizableNodeIdentifier Identifier of the authorizable node
     * @param groupNodeIds Map to receive the node ids of the groups
     * @param session the session to read from
     * @throws RepositoryException if an error occurs
     */
    private void memberOf(String authorizableNodeIdentifier, Collection<String> groupNodeIds, Session session) throws RepositoryException {
        Collection<String> declared = collectDeclaredMembership(authorizableNodeIdentifier, session);
        for (String identifier : declared) {
            if (groupNodeIds.add(identifier)) {
                memberOf(identifier, groupNodeIds, session);
            }
        }
    }

    /**
     * Collects the declared memberships for the given authorizable by resolving the week references to the authorizable.
     * If the lookup fails, <code>null</code> is returned. This most likely the case if the authorizable does not exit (yet)
     * in the  session that is used for the lookup.
     *
     * @param authorizableNodeIdentifier Identifier of the authorizable node
     * @param session the session to read from
     * @return a collection of group node ids or <code>null</code> if the lookup failed.
     * @throws RepositoryException if an error occurs
     */
    private Collection<String> collectDeclaredMembershipFromReferences(String authorizableNodeIdentifier,
                                                                       Session session) throws RepositoryException {
        Set<String> pIds = new HashSet<String>();
        Set<String> nIds = new HashSet<String>();

        // Try to get membership information from references
        PropertyIterator refs = getMembershipReferences(authorizableNodeIdentifier, session);
        if (refs == null) {
            return null;
        }

        while (refs.hasNext()) {
            try {
                PropertyImpl pMember = (PropertyImpl) refs.nextProperty();
                NodeImpl nGroup = (NodeImpl) pMember.getParent();

                Set<String> groupNodeIdentifiers;
                if (P_MEMBERS.equals(pMember.getQName())) {
                    // Found membership information in members property
                    groupNodeIdentifiers = pIds;
                } else {
                    // Found membership information in members node
                    groupNodeIdentifiers = nIds;
                    while (nGroup.isNodeType(NT_REP_MEMBERS)) {
                        nGroup = (NodeImpl) nGroup.getParent();
                    }
                }

                if (nGroup.isNodeType(NT_REP_GROUP)) {
                    groupNodeIdentifiers.add(nGroup.getIdentifier());
                } else {
                    // weak-ref property 'rep:members' that doesn't reside under an
                    // group node -> doesn't represent a valid group member.
                    log.debug("Invalid member reference to '{}' -> Not included in membership set.", this);
                }
            } catch (ItemNotFoundException e) {
                // group node doesn't exist  -> -> ignore exception
                // and skip this reference from membership list.
            } catch (AccessDeniedException e) {
                // not allowed to see the group node -> ignore exception
                // and skip this reference from membership list.
            }
        }

        // Based on the user's setting return either of the found membership information
        return select(pIds, nIds);
    }

    /**
     * Collects the declared memberships for the given authorizable by traversing the groups structure.
     *
     * @param authorizableNodeIdentifier Identifier of the authorizable node
     * @param session the session to read from
     * @return a collection of group node ids.
     * @throws RepositoryException if an error occurs
     */
    private Collection<String> collectDeclaredMembershipFromTraversal(
            final String authorizableNodeIdentifier, Session session) throws RepositoryException {

        final Set<String> pIds = new HashSet<String>();
        final Set<String> nIds = new HashSet<String>();

        // workaround for failure of Node#getWeakReferences
        // traverse the tree below groups-path and collect membership manually.
        log.info("Traversing groups tree to collect membership.");
        if (session.nodeExists(groupsPath)) {
            Node groupsNode = session.getNode(groupsPath);
            traverseAndCollect(authorizableNodeIdentifier, pIds, nIds, (NodeImpl) groupsNode);
        } // else: no groups exist -> nothing to do.

        // Based on the user's setting return either of the found membership information
        return select(pIds, nIds);
    }

    /**
     * traverses the groups structure to find the groups of which the given authorizable is member of.
     *
     * @param authorizableNodeIdentifier Identifier of the authorizable node
     * @param pIds output set to update of group node ids that were found via the property memberships
     * @param nIds output set to update of group node ids that were found via the node memberships
     * @param node the node to traverse
     * @throws RepositoryException if an error occurs
     */
    private void traverseAndCollect(String authorizableNodeIdentifier, Set<String> pIds, Set<String> nIds, NodeImpl node)
            throws RepositoryException {
        if (node.isNodeType(NT_REP_GROUP)) {
            String groupId = node.getIdentifier();
            if (node.hasProperty(P_MEMBERS)) {
                for (Value value : node.getProperty(P_MEMBERS).getValues()) {
                    String v = value.getString();
                    if (v.equals(authorizableNodeIdentifier)) {
                        pIds.add(groupId);
                    }
                }
            }
            NodeIterator iter = node.getNodes();
            while (iter.hasNext()) {
                NodeImpl child = (NodeImpl) iter.nextNode();
                if (child.isNodeType(NT_REP_MEMBERS)) {
                    isMemberOfNodeBaseMembershipGroup(authorizableNodeIdentifier, groupId, nIds, child);
                }
            }
        } else {
            NodeIterator iter = node.getNodes();
            while (iter.hasNext()) {
                NodeImpl child = (NodeImpl) iter.nextNode();
                traverseAndCollect(authorizableNodeIdentifier, pIds, nIds, child);
            }
        }
    }

    /**
     * traverses the group structure of a node-based group to check if the given authorizable is member of this group.
     *
     * @param authorizableNodeIdentifier Identifier of the authorizable node
     * @param groupId if of the group
     * @param nIds output set to update of group node ids that were found via the node memberships
     * @param node the node to traverse
     * @throws RepositoryException if an error occurs
     */
    private void isMemberOfNodeBaseMembershipGroup(String authorizableNodeIdentifier, String groupId, Set<String> nIds,
                                                   NodeImpl node)
            throws RepositoryException {
        PropertyIterator pIter = node.getProperties();
        while (pIter.hasNext()) {
            PropertyImpl p = (PropertyImpl) pIter.nextProperty();
            if (p.getType() == PropertyType.WEAKREFERENCE) {
                Value[] values = p.isMultiple()
                        ? p.getValues()
                        : new Value[]{p.getValue()};
                for (Value v: values) {
                    if (v.getString().equals(authorizableNodeIdentifier)) {
                        nIds.add(groupId);
                        return;
                    }
                }
            }
        }
        NodeIterator iter = node.getNodes();
        while (iter.hasNext()) {
            NodeImpl child = (NodeImpl) iter.nextNode();
            if (child.isNodeType(NT_REP_MEMBERS)) {
                isMemberOfNodeBaseMembershipGroup(authorizableNodeIdentifier, groupId, nIds, child);
            }
        }
    }

    /**
     * Return either of both sets depending on the users setting whether
     * to use the members property or the members node to record membership
     * information. If both sets are non empty, the one configured in the
     * settings will take precedence and an warning is logged.
     *
     * @param pIds the set of group node ids retrieved through membership properties
     * @param nIds the set of group node ids retrieved through membership nodes
     * @return the selected set.
     */
    private Set<String> select(Set<String> pIds, Set<String> nIds) {
        Set<String> result;
        if (useMembersNode) {
            if (!nIds.isEmpty() || pIds.isEmpty()) {
                result = nIds;
            } else {
                result = pIds;
            }
        } else {
            if (!pIds.isEmpty() || nIds.isEmpty()) {
                result = pIds;
            } else {
                result = nIds;
            }
        }

        if (!pIds.isEmpty() && !nIds.isEmpty()) {
            log.warn("Found members node and members property. Ignoring {} members", useMembersNode ? "property" : "node");
        }

        return result;
    }


    /**
     * @return a new Session that needs to be properly released after usage.
     */
    private SessionImpl getSession() {
        try {
            return (SessionImpl) systemSession.createSession(systemSession.getWorkspace().getName());
        } catch (RepositoryException e) {
            // fallback
            return systemSession;
        }
    }

    /**
     * Returns the membership references for the given authorizable.
     * @param authorizableNodeIdentifier Identifier of the authorizable node
     * @param session session to read from
     * @return the property iterator or <code>null</code>
     */
    private static PropertyIterator getMembershipReferences(String authorizableNodeIdentifier, Session session) {
        PropertyIterator refs = null;
        try {
            refs = session.getNodeByIdentifier(authorizableNodeIdentifier).getWeakReferences(null);
        } catch (RepositoryException e) {
            log.error("Failed to retrieve membership references of " + authorizableNodeIdentifier + ".", e);
        }
        return refs;
    }


    // --------------------------------------------------------------------------
    // todo: merge with GroupImpl code

    private MembershipProvider getMembershipProvider(NodeImpl node) throws RepositoryException {
        MembershipProvider msp;
        if (useMembersNode) {
            if (node.hasNode(N_MEMBERS) || !node.hasProperty(P_MEMBERS)) {
                msp = new NodeBasedMembershipProvider(node);
            } else {
                msp = new PropertyBasedMembershipProvider(node);
            }
        } else {
            msp = new PropertyBasedMembershipProvider(node);
        }

        if (node.hasProperty(P_MEMBERS) && node.hasNode(N_MEMBERS)) {
            log.warn("Found members node and members property on node {}. Ignoring {} members", node, useMembersNode ? "property" : "node");
        }
        return msp;
    }

    /**
     * Inner MembershipProvider interface
     */
    private interface MembershipProvider {

        Iterator<String> getDeclaredMemberReferences() throws RepositoryException;
    }

    /**
     * PropertyBasedMembershipProvider
     */
    private class PropertyBasedMembershipProvider implements MembershipProvider {
        private final NodeImpl node;

        private PropertyBasedMembershipProvider(NodeImpl node) {
            super();
            this.node = node;
        }

        public Iterator<String> getDeclaredMemberReferences() throws RepositoryException {
            if (node.hasProperty(P_MEMBERS)) {
                final Value[] members = node.getProperty(P_MEMBERS).getValues();

                return new Iterator<String>() {

                    private int pos;

                    @Override
                    public boolean hasNext() {
                        return pos < members.length;
                    }

                    @Override
                    public String next() {
                        if (pos < members.length) {
                            try {
                                return members[pos++].getString();
                            } catch (RepositoryException e) {
                                throw new IllegalStateException(e);
                            }
                        } else {
                            throw new NoSuchElementException();
                        }
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };

            } else {
                return Iterators.empty();
            }
        }
    }

    /**
     * NodeBasedMembershipProvider
     */
    private class NodeBasedMembershipProvider implements MembershipProvider {
        private final NodeImpl node;

        private NodeBasedMembershipProvider(NodeImpl node) {
            this.node = node;
        }

        public Iterator<String> getDeclaredMemberReferences() throws RepositoryException {
            if (node.hasNode(N_MEMBERS)) {
                final Iterator<Property> members = getPropertySequence(node.getNode(N_MEMBERS)).iterator();

                return new Iterator<String>() {
                    @Override
                    public boolean hasNext() {
                        return members.hasNext();
                    }

                    @Override
                    public String next() {
                        try {
                            return members.next().getString();
                        } catch (RepositoryException e) {
                            throw new IllegalStateException(e);
                        }
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            } else {
                return Iterators.empty();
            }
        }

    }

    static PropertySequence getPropertySequence(Node nMembers) throws RepositoryException {
        Comparator<String> order = Rank.comparableComparator();
        int maxChildren = 4;
        int minChildren = maxChildren / 2;
        TreeManager treeManager = new BTreeManager(nMembers, minChildren, maxChildren, order, false);
        return ItemSequence.createPropertySequence(treeManager);
    }


}