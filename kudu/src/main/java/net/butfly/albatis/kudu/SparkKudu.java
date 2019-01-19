package net.butfly.albatis.kudu;
import java.util.Map;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;

public interface SparkKudu {
	default Map<String, String> kuduOpts(URISpec targetUri) {
		String kududbn = targetUri.getPathAt(0);
		String kuducol = targetUri.getPathAt(1);
		String kuduuri = targetUri.getAuthority();
		String kuduMasterStr = targetUri.getHost();
		Logger.getLogger(getClass()).info("sparkKudu connecting to: " + targetUri.toString());
		return Maps.of("uri", kuduuri //
				, "database", kududbn//
				, "kudu.master", kuduMasterStr//
		);
	}
}