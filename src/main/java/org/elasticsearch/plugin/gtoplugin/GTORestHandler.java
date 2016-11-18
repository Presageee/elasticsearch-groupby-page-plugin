package org.elasticsearch.plugin.gtoplugin;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugin.gto.ResponseAction;
import org.elasticsearch.rest.*;

/**
 * Created by LJT on 16-11-17.
 * email: linjuntan@sensetime.com
 */
public class GTORestHandler extends BaseRestHandler {
    @Inject
    public GTORestHandler(Settings settings,  Client client, RestController controller) {
        super(settings, controller, client);
        controller.registerHandler(RestRequest.Method.GET, "/groupBy", this);
        controller.registerHandler(RestRequest.Method.POST, "/groupBy", this);
        controller.registerHandler(RestRequest.Method.GET, "/hello", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        if (request.uri().contains("hello")) {
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, "hello"));
        }
        String param = request.param("req");
        if (param == null) {
            param = request.content().toUtf8();
        }
        ResponseAction responseAction = new ResponseAction();
        try {
            responseAction.execute(client, channel, param);
        } catch (Exception e) {
            channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, "Json parse error or type error"));
        }
    }
}
