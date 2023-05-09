/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.proxy.frontend.postgresql.authentication;

import com.google.common.base.Strings;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslHandler;
import org.apache.shardingsphere.authority.checker.AuthorityChecker;
import org.apache.shardingsphere.authority.rule.AuthorityRule;
import org.apache.shardingsphere.db.protocol.constant.CommonConstants;
import org.apache.shardingsphere.db.protocol.constant.DatabaseProtocolServerInfo;
import org.apache.shardingsphere.db.protocol.payload.PacketPayload;
import org.apache.shardingsphere.db.protocol.postgresql.constant.PostgreSQLAuthenticationMethod;
import org.apache.shardingsphere.db.protocol.postgresql.packet.generic.PostgreSQLReadyForQueryPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.handshake.PostgreSQLAuthenticationOKPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.handshake.PostgreSQLComStartupPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.handshake.PostgreSQLParameterStatusPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.handshake.PostgreSQLPasswordMessagePacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.handshake.PostgreSQLRandomGenerator;
import org.apache.shardingsphere.db.protocol.postgresql.packet.handshake.PostgreSQLSSLUnwillingPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.handshake.PostgreSQLSSLWillingPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.handshake.authentication.PostgreSQLMD5PasswordAuthenticationPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.handshake.authentication.PostgreSQLPasswordAuthenticationPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.identifier.PostgreSQLIdentifierPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.identifier.PostgreSQLMessagePacketType;
import org.apache.shardingsphere.db.protocol.postgresql.payload.PostgreSQLPacketPayload;
import org.apache.shardingsphere.dialect.exception.syntax.database.UnknownDatabaseException;
import org.apache.shardingsphere.dialect.postgresql.exception.authority.EmptyUsernameException;
import org.apache.shardingsphere.dialect.postgresql.exception.authority.InvalidPasswordException;
import org.apache.shardingsphere.dialect.postgresql.exception.authority.PrivilegeNotGrantedException;
import org.apache.shardingsphere.dialect.postgresql.exception.authority.UnknownUsernameException;
import org.apache.shardingsphere.dialect.postgresql.exception.protocol.ProtocolViolationException;
import org.apache.shardingsphere.infra.metadata.user.Grantee;
import org.apache.shardingsphere.infra.metadata.user.ShardingSphereUser;
import org.apache.shardingsphere.infra.util.exception.ShardingSpherePreconditions;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.postgresql.handler.admin.PostgreSQLCharacterSets;
import org.apache.shardingsphere.proxy.frontend.authentication.AuthenticationEngine;
import org.apache.shardingsphere.proxy.frontend.authentication.AuthenticationResult;
import org.apache.shardingsphere.proxy.frontend.authentication.AuthenticationResultBuilder;
import org.apache.shardingsphere.proxy.frontend.authentication.Authenticator;
import org.apache.shardingsphere.proxy.frontend.authentication.AuthenticatorFactory;
import org.apache.shardingsphere.proxy.frontend.connection.ConnectionIdGenerator;
import org.apache.shardingsphere.proxy.frontend.postgresql.authentication.authenticator.PostgreSQLAuthenticatorType;
import org.apache.shardingsphere.proxy.frontend.ssl.ProxySSLContext;

import java.util.Optional;

/**
 * Authentication engine for PostgreSQL.
 */
public final class PostgreSQLAuthenticationEngine implements AuthenticationEngine {
    
    private static final int SSL_REQUEST_PAYLOAD_LENGTH = 8;
    
    private static final int SSL_REQUEST_CODE = (1234 << 16) + 5679;
    
    private boolean startupMessageReceived;
    
    private String clientEncoding;
    
    private byte[] md5Salt;
    
    private AuthenticationResult currentAuthResult;
    
    @Override
    public int handshake(final ChannelHandlerContext context) {
        return ConnectionIdGenerator.getInstance().nextId();
    }
    
    @Override
    public AuthenticationResult authenticate(final ChannelHandlerContext context, final PacketPayload payload) {
        if (SSL_REQUEST_PAYLOAD_LENGTH == payload.getByteBuf().markReaderIndex().readInt() && SSL_REQUEST_CODE == payload.getByteBuf().readInt()) {
            if (ProxySSLContext.getInstance().isSSLEnabled()) {
                SslHandler sslHandler = new SslHandler(ProxySSLContext.getInstance().newSSLEngine(context.alloc()), true);
                context.pipeline().addFirst(SslHandler.class.getSimpleName(), sslHandler);
                context.writeAndFlush(new PostgreSQLSSLWillingPacket());
            } else {
                context.writeAndFlush(new PostgreSQLSSLUnwillingPacket());
            }
            return AuthenticationResultBuilder.continued();
        }
        payload.getByteBuf().resetReaderIndex();
        AuthorityRule rule = ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData().getGlobalRuleMetaData().getSingleRule(AuthorityRule.class);
        return startupMessageReceived ? processPasswordMessage(context, (PostgreSQLPacketPayload) payload, rule) : processStartupMessage(context, (PostgreSQLPacketPayload) payload, rule);
    }
    
    private AuthenticationResult processPasswordMessage(final ChannelHandlerContext context, final PostgreSQLPacketPayload payload, final AuthorityRule rule) {
        char messageType = (char) payload.readInt1();
        ShardingSpherePreconditions.checkState(PostgreSQLMessagePacketType.PASSWORD_MESSAGE.getValue() == messageType,
                () -> new ProtocolViolationException("password", Character.toString(messageType)));
        PostgreSQLPasswordMessagePacket passwordMessagePacket = new PostgreSQLPasswordMessagePacket(payload);
        login(currentAuthResult.getDatabase(), currentAuthResult.getUsername(), md5Salt, passwordMessagePacket.getDigest(), rule);
        // TODO implement PostgreSQLServerInfo like MySQLServerInfo
        context.write(new PostgreSQLAuthenticationOKPacket());
        context.write(new PostgreSQLParameterStatusPacket("server_version", DatabaseProtocolServerInfo.getProtocolVersion(currentAuthResult.getDatabase(), "PostgreSQL")));
        context.write(new PostgreSQLParameterStatusPacket("client_encoding", clientEncoding));
        context.write(new PostgreSQLParameterStatusPacket("server_encoding", "UTF8"));
        context.write(new PostgreSQLParameterStatusPacket("integer_datetimes", "on"));
        context.write(new PostgreSQLParameterStatusPacket("standard_conforming_strings", "on"));
        context.writeAndFlush(PostgreSQLReadyForQueryPacket.NOT_IN_TRANSACTION);
        return AuthenticationResultBuilder.finished(currentAuthResult.getUsername(), "", currentAuthResult.getDatabase());
    }
    
    private void login(final String databaseName, final String username, final byte[] md5Salt, final String digest, final AuthorityRule rule) {
        ShardingSpherePreconditions.checkState(Strings.isNullOrEmpty(databaseName) || ProxyContext.getInstance().databaseExists(databaseName), () -> new UnknownDatabaseException(databaseName));
        Grantee grantee = new Grantee(username, "%");
        Optional<ShardingSphereUser> user = rule.findUser(grantee);
        ShardingSpherePreconditions.checkState(user.isPresent(), () -> new UnknownUsernameException(username));
        ShardingSpherePreconditions.checkState(new AuthenticatorFactory<>(PostgreSQLAuthenticatorType.class, rule).newInstance(user.get()).authenticate(user.get(), new Object[]{digest, md5Salt}),
                () -> new InvalidPasswordException(username));
        ShardingSpherePreconditions.checkState(null == databaseName || new AuthorityChecker(rule, grantee).isAuthorized(databaseName), () -> new PrivilegeNotGrantedException(username, databaseName));
    }
    
    private AuthenticationResult processStartupMessage(final ChannelHandlerContext context, final PostgreSQLPacketPayload payload, final AuthorityRule rule) {
        startupMessageReceived = true;
        PostgreSQLComStartupPacket startupPacket = new PostgreSQLComStartupPacket(payload);
        clientEncoding = startupPacket.getClientEncoding();
        context.channel().attr(CommonConstants.CHARSET_ATTRIBUTE_KEY).set(PostgreSQLCharacterSets.findCharacterSet(clientEncoding));
        String username = startupPacket.getUsername();
        ShardingSpherePreconditions.checkState(!Strings.isNullOrEmpty(username), EmptyUsernameException::new);
        context.writeAndFlush(getIdentifierPacket(username, rule));
        currentAuthResult = AuthenticationResultBuilder.continued(username, "", startupPacket.getDatabase());
        return currentAuthResult;
    }
    
    private PostgreSQLIdentifierPacket getIdentifierPacket(final String username, final AuthorityRule rule) {
        Optional<Authenticator> authenticator = rule.findUser(new Grantee(username, "")).map(optional -> new AuthenticatorFactory<>(PostgreSQLAuthenticatorType.class, rule).newInstance(optional));
        if (authenticator.isPresent() && PostgreSQLAuthenticationMethod.PASSWORD == authenticator.get().getAuthenticationMethod()) {
            return new PostgreSQLPasswordAuthenticationPacket();
        }
        md5Salt = PostgreSQLRandomGenerator.getInstance().generateRandomBytes(4);
        return new PostgreSQLMD5PasswordAuthenticationPacket(md5Salt);
    }
}
