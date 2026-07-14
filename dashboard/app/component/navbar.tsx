export default function Navbar() {
    return (
        <>
        <div className="bg-[#101010] text-white p-4 w-full max-h-16 flex items-center justify-between">
            <div className="flex gap-3">
                <h1 className="text-xl font-bold">Datafeed</h1>
                <button className="bg-[#161616] rounded px-4"></button>
                <button className="bg-[#161616] rounded px-4">Button 2</button>
                <button className="bg-[#161616] rounded px-4">Button 3</button>
                <button className="bg-[#161616] rounded px-4">Button 4</button>
                <button className="bg-[#161616] rounded px-4">History</button>
                <button className="bg-[#161616] rounded px-4">Tools</button>
                <button className="bg-[#161616] rounded px-4">Windows</button>
                <button className="bg-[#161616] rounded px-4">Layouts</button>
                <button className="bg-[#161616] rounded px-4">Help</button>
            </div>
            <div className="flex gap-3">
                <button className="bg-[#161616] rounded px-4">Uptime 148h</button>
                <button className="bg-[#161616] rounded px-4">Exchange 4</button>
                <button className="bg-[#161616] rounded px-4">Network up</button>
                <button className="bg-[#161616] rounded px-4">Health up</button>
                <button className="bg-[#161616] rounded px-4">Connections 8</button>
                <button className="bg-[#161616] rounded px-4">Latency 129ms</button>
                <button className="bg-[#161616] rounded px-4">Alerts 3</button>
                <button className="bg-[#161616] rounded px-4">Param</button>
            </div>
        </div>    
        </>
    );
}
